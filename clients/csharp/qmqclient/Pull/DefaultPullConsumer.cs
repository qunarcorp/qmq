using Qunar.TC.Qmq.Client.Consumer;
using Qunar.TC.Qmq.Client.Exceptions;
using Qunar.TC.Qmq.Client.NewQmq;
using Qunar.TC.Qmq.Client.tag;
using Qunar.TC.Qmq.Client.Util;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Qunar.TC.Qmq.Client.Pull
{
    internal class DefaultPullConsumer : IPullConsumer
    {
        private static readonly NLog.Logger Logger = NLog.LogManager.GetCurrentClassLogger();

        private static readonly TimeSpan MinPullTimeout = TimeSpan.FromMilliseconds(300);

        private readonly PullController _controller;
        private readonly Puller[] _pullers = new Puller[2];

        private readonly object _lockHelper = new object();

        private readonly ConcurrentQueue<Message> _localBuffer;

        private const int LOCAL_BUFFER_SIZE = 50;

        internal DefaultPullConsumer(string subject, string group, ConsumeMode mode, bool isBroadcast, string appCode, string metaServer, TagType tagType, string[] tags)
        {
            CheckArgument(subject, group, isBroadcast);

            if (isBroadcast)
            {
                group = ClientId.CurrentUniqeConsumerGroup;
            }

            Subject = subject;
            ConsumerGroup = group;
            ConsumeMode = mode;
            IsBroadcast = isBroadcast;

            _localBuffer = new ConcurrentQueue<Message>();

            _controller = new PullController(appCode, metaServer);
            var retrySubject = RetrySubjectUtils.BuildRetrySubject(subject, group);
            _pullers[0] = new Puller(retrySubject, group, isBroadcast, tagType, tags, mode, _controller, new WeightPullStrategy(), new RoundRobinLoadBalance());
            _pullers[1] = new Puller(subject, group, isBroadcast, tagType, tags, mode, _controller, new AlwaysPullStrategy(), new WeightLoadBalance());
        }

        public List<Message> Pull(int expectNum, TimeSpan timeout)
        {
            try
            {
                lock (_lockHelper)
                {
                    var bufferSize = _localBuffer.Count();
                    if (bufferSize == 0)
                    {
                        return ProcessEmptyLocalBuffer(expectNum, timeout);
                    }

                    if (bufferSize >= expectNum)
                    {
                        var list = new List<Message>(Dequeue(_localBuffer, expectNum));
                        LogPulledQueueTime(list);
                        return list;
                    }

                    var fetchSize = Math.Max((expectNum - bufferSize), LOCAL_BUFFER_SIZE);
                    var messages = AsyncPull(fetchSize, timeout).Result;


                    List<Message> result = new List<Message>(Dequeue(_localBuffer, bufferSize));
                    var takeSize = Math.Min(expectNum - bufferSize, messages.Count);
                    result.AddRange(messages.Take(takeSize));
                    var remain = messages.Skip(takeSize);
                    foreach (var item in remain)
                    {
                        _localBuffer.Enqueue(item);
                    }
                    LogPulledQueueTime(result);
                    return result;

                }
            }
            catch (Exception e)
            {
                return new List<Message>();
            }
            finally
            {
            }

        }

        private List<Message> ProcessEmptyLocalBuffer(int expectNum, TimeSpan timeout)
        {
            var fetchSize = Math.Max(expectNum, LOCAL_BUFFER_SIZE);
            var messages = AsyncPull(fetchSize, timeout).Result;

            var remain = messages.Skip(expectNum);
            foreach (var item in remain)
            {
                _localBuffer.Enqueue(item);
            }
            var result = new List<Message>(messages.Take(expectNum));
            LogPulledQueueTime(result);
            return result;
        }

        private IEnumerable<Message> Dequeue(ConcurrentQueue<Message> queue, int size)
        {
            for (var i = 0; i < size; ++i)
            {
                if (_localBuffer.TryDequeue(out var item))
                {
                    yield return item;
                }
                else
                {
                    yield break;
                }
            }
        }

        public Task<List<Message>> AsyncPull(int expectNum, TimeSpan timeout)
        {
            if (expectNum <= 0)
            {
                var tsc = new TaskCompletionSource<List<Message>>();
                tsc.SetResult(new List<Message>());
                return tsc.Task;
            }

            if (timeout != TimeSpan.MinValue && timeout < MinPullTimeout)
            {
                timeout = MinPullTimeout;
            }

            return Task<List<Message>>.Factory.ContinueWhenAll(StartPull(expectNum, timeout), tasks =>
            {
                var allTaskFailed = true;
                Exception exception = null;
                var messages = new List<Message>(expectNum);
                foreach (var task in tasks)
                {
                    var result = task.Result;
                    if (result.Success)
                    {
                        allTaskFailed = false;
                        messages.AddRange(result.Messages);
                    }
                    else
                    {
                        var ex = result.Exception;
                        if (ex is TimeoutException)
                        {
                            Logger.Warn(ex, $"pull message failed. {Subject}/{ConsumerGroup}, error message: {result.ErrorMessage}");
                        }
                        else if (!(ex is BrokerUnassignedException || ex is NoWritableBrokerException))
                        {
                            Logger.Error(ex, $"pull message failed. {Subject}/{ConsumerGroup}, error message: {result.ErrorMessage}");
                        }

                        if (exception == null)
                        {
                            exception = ex;
                        }
                    }
                }

                if (allTaskFailed && exception != null)
                {
                    throw exception;
                }

                return messages;
            });
        }

        private Task<PullResult>[] StartPull(int expectNum, TimeSpan timeout)
        {
            var tasks = new Task<PullResult>[_pullers.Length];
            for (var i = 0; i < _pullers.Length; i++)
            {
                tasks[i] = _pullers[i].Pull(expectNum, timeout);
            }

            return tasks;
        }

        private static void CheckArgument(string subject, string group, bool isBroadcast)
        {
            if (string.IsNullOrEmpty(subject))
            {
                throw new ArgumentException("subject不能为空");
            }
            if (isBroadcast && !string.IsNullOrEmpty(group))
            {
                throw new ArgumentException("广播方式消费时请将group设为空");
            }
            if (!isBroadcast && string.IsNullOrEmpty(group))
            {
                throw new ArgumentException("group不能为空");
            }
        }

        public string Subject { get; }

        public string ConsumerGroup { get; }

        public bool IsBroadcast { get; }

        public ConsumeMode ConsumeMode { get; }

        private void LogPulledQueueTime(List<Message> messages)
        {
            try
            {
                foreach (var message in messages)
                {
                    if (message is PulledMessage pulledMessage)
                    {
                        pulledMessage.SetStart(DateTime.Now.ToTime());
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }
    }
}
