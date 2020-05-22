using System;
using System.Collections;
using System.Threading;
using System.Threading.Tasks;
using Qunar.TC.Qmq.Client.Codec;
using Qunar.TC.Qmq.Client.Exceptions;
using Qunar.TC.Qmq.Client.Model;
using Qunar.TC.Qmq.Client.Util;

namespace Qunar.TC.Qmq.Client.NewQmq
{
    internal class AckHandler
    {
        private static readonly NLog.Logger Logger = NLog.LogManager.GetCurrentClassLogger();

        private const int MaxRetryTimes = 100;
        private const int RetryIntervalMillis = 500;

        private const int Idle = 0;
        private const int Working = 1;

        private readonly BaseMessage _message;
        private readonly AckHandlerQueue _workingQueue;
        private volatile AckHandler _next;

        private int _state = Idle;

        public AckHandler(long pullLogOffset, BaseMessage message, AckHandlerQueue workingQueue)
        {
            PullLogOffset = pullLogOffset;
            _message = message;
            _workingQueue = workingQueue;

        }

        public long PullLogOffset { get; }

        public AckHandler Next
        {
            get => _next;
            set => _next = value;
        }

        public bool Completed { get; private set; } = false;

        public void Ack(long elapsed, Exception e)
        {
            if (Interlocked.CompareExchange(ref _state, Working, Idle) != Idle)
            {
                return;
            }

            if (e == null)
            {
                Ack();
                return;
            }


            if (IsSendDealyAck(e))
            {
                var next = ((NeedRetryException)e).Next;
                var nextTime = DateTime.Now + next;
                var message = CreateDelayMessage(nextTime);
                DelayAck(message, nextTime);

            }
            else
            {
                var message = CreateRetryMessage();
                NAck(message);
            }
        }

        private bool IsSendDealyAck(Exception e)
        {
            var retryException = e as NeedRetryException;
            if (retryException == null) return false;

            if (Times() > MaxRetryTimes) return false;
            var next = retryException.Next;
            if (next <= TimeSpan.FromMilliseconds(50)) return false;

            return true;

        }

        private BaseMessage CreateDelayMessage(DateTime nextTime)
        {
            var delayMessage = new BaseMessage(_message);
            delayMessage.SetPropertyForInternal(BaseMessage.keys.qmq_createTime.ToString(), DateTime.Now.ToTime());
            delayMessage.SetPropertyForInternal(BaseMessage.keys.qmq_times.ToString(), Times() + 1);
            delayMessage.SetDelayTime(nextTime);
            return delayMessage;
        }

        private BaseMessage CreateRetryMessage()
        {
            var retrySubject = BuildRetrySubject();
            var retryMessage = new BaseMessage(retrySubject, _message);
            retryMessage.SetPropertyForInternal(BaseMessage.keys.qmq_createTime.ToString(), DateTime.Now.ToTime());
            retryMessage.SetPropertyForInternal(BaseMessage.keys.qmq_times.ToString(), Times() + 1);
            return retryMessage;
        }

        private string BuildRetrySubject()
        {
            var realSubject = RetrySubjectUtils.RealSubject(_workingQueue.Subject);
            if (Times() > MaxRetryTimes)
            {
                return RetrySubjectUtils.BuildDeadRetrySubject(realSubject, _workingQueue.Group);
            }
            else
            {
                return RetrySubjectUtils.BuildRetrySubject(realSubject, _workingQueue.Group);
            }
        }

        private int Times()
        {
            var times = _message.GetStringProperty(BaseMessage.keys.qmq_times);
            return string.IsNullOrEmpty(times) ? 0 : int.Parse(times);
        }

        private void Ack()
        {
            Completed = true;
            _workingQueue.HandleAck(this);
        }

        private void NAck(BaseMessage message, bool forceRefreshBrokerGroup = false)
        {
            _workingQueue.SendMessageBackService.SendBack(message.Subject, message, forceRefreshBrokerGroup)
                .ContinueWith(task => { HandleSendBackResponse(message, task.Result); }, TaskScheduler.Default);
        }

        private void HandleSendBackResponse(BaseMessage message, Response resp)
        {
            if (!resp.IsOk())
            {
                Logger.Warn("qmq nack send message back failed(response error) will retry until success");
                Delay(RetryIntervalMillis).ContinueWith(_ => NAck(message, true));
                return;
            }

            var result = (Hashtable)resp.Result;
            if (result.Count == 0)
            {
                Ack();
            }
            else
            {
                Logger.Warn("qmq nack send message back failed(one message failed), will retry until success");
                Delay(RetryIntervalMillis).ContinueWith(_ => NAck(message, true));
            }
        }

        private static Task<object> Delay(int milliseconds)
        {
            var tcs = new TaskCompletionSource<object>();
            new Timer(_ => tcs.SetResult(null)).Change(milliseconds, -1);
            return tcs.Task;
        }

        private void DelayAck(BaseMessage message, DateTime? nextTime = null, bool forceRefreshBrokerGroup = false)
        {
            _workingQueue.SendMessageBackService.SendBack(message.Subject, message, forceRefreshBrokerGroup, true)
                 .ContinueWith(task => { HandleDelaySendBackResponse(message, task.Result); }, TaskScheduler.Default);
        }

        private void HandleDelaySendBackResponse(BaseMessage message, Response resp, bool isDelayMessage = false)
        {
            if (!resp.IsOk())
            {
                Logger.Warn("qmq nack send message back failed(response error) will retry until success");
                Delay(RetryIntervalMillis).ContinueWith(_ => RetryDelay(message));
                return;
            }

            var result = (Hashtable)resp.Result;
            if (result.Count == 0)
            {
                Ack();
            }
            else
            {
                Logger.Warn("qmq nack send message back failed(one message failed), will retry until success");
                Delay(RetryIntervalMillis).ContinueWith(_ => RetryDelay(message));
            }
        }

        private void RetryDelay(BaseMessage message)
        {
            var deadline = message.ScheduleReceiveTime;
            if (deadline.HasValue)
            {
                var span = deadline.Value - DateTime.Now;
                if (span > TimeSpan.Zero)
                {
                    DelayAck(message, forceRefreshBrokerGroup: true);
                }
                else
                {
                    NAck(message, true);
                }
            }
        }
    }
}