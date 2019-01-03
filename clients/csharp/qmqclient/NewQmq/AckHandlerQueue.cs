using Qunar.TC.Qmq.Client.Exceptions;
using Qunar.TC.Qmq.Client.Model;
using Qunar.TC.Qmq.Client.NewQmq.Model;
using Qunar.TC.Qmq.Client.Util;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NLog;

namespace Qunar.TC.Qmq.Client.NewQmq
{
    internal class AckHandlerQueue
    {
        private static readonly Logger LOG = LogManager.GetCurrentClassLogger();

        private const int MinAckBatch = 20;
        private const int TryForceSendAckIntervalSeconds = 10;

        private readonly string _brokerGroup;
        private readonly bool _isBroadcast;
        private readonly AckMessageService _ackMessageService;

        private readonly object _updateGuard = new object();
        private AckHandler _head;
        private AckHandler _tail;
        private AckHandler _beginScanPosition;
        private long _lastAckTime;

        private readonly ConcurrentQueue<AckRequest> _ackRequestQueue;

        private readonly object _sendingGuard = new object();
        private volatile bool _sendingAck = false;

        public AckHandlerQueue(string subject, string group, string brokerGroup, bool isBroadcast, AckMessageService ackMessageService, SendMessageBackService sendMessageBackService)
        {
            Subject = subject;
            Group = group;
            _brokerGroup = brokerGroup;
            _isBroadcast = isBroadcast;
            _ackMessageService = ackMessageService;
            _ackRequestQueue = new ConcurrentQueue<AckRequest>();
            SendMessageBackService = sendMessageBackService;

            MinPullOffset = -1;
            MaxPullOffset = -1;

            Scheduler.INSTANCE.ScheduleAtFixDelay(ForceSendAck, TryForceSendAckIntervalSeconds);
        }

        public long MinPullOffset { get; private set; }

        public long MaxPullOffset { get; private set; }

        public string Subject { get; }

        public string Group { get; }

        public SendMessageBackService SendMessageBackService { get; }

        public List<AckHandler> AcquireHandlers(IList<BaseMessage> messages)
        {
            var handlers = new List<AckHandler>(messages.Count);
            lock (_updateGuard)
            {
                handlers.AddRange(messages.Select(AckquireHandler));
            }
            return handlers;
        }

        private AckHandler AckquireHandler(BaseMessage message)
        {
            lock (_updateGuard)
            {
                var pullLogOffset = PullLogOffset(message);

                var handler = new AckHandler(pullLogOffset, message, this);
                if (_head == null)
                {
                    _beginScanPosition = _head = handler;
                    MinPullOffset = pullLogOffset;
                }
                if (_tail != null)
                {
                    WarningIfOffsetNotContinuous(_tail, handler);
                    _tail.Next = handler;
                }

                _tail = handler;
                MaxPullOffset = pullLogOffset;

                return handler;
            }
        }

        private void WarningIfOffsetNotContinuous(AckHandler tail, AckHandler handler)
        {
            if (tail.PullLogOffset + 1 != handler.PullLogOffset)
            {
                LOG.Warn($"Qmq.Consume.PullOffsetNotContinuous {Subject}/{Group}/{_brokerGroup}");
            }
        }

        private static long PullLogOffset(Message message)
        {
            return long.Parse(message.GetStringProperty(BaseMessage.keys.qmq_pullOffset.ToString()));
        }

        private void ForceSendAck()
        {
            bool needSendRequests = HandleForceSendAckRequests();
            if (needSendRequests)
            {
                TriggerAckSender();
            }
        }

        private bool HandleForceSendAckRequests()
        {
            lock (_updateGuard)
            {
                if (DateTime.Now.ToTime() - _lastAckTime < TryForceSendAckIntervalSeconds * 1000)
                {
                    return false;
                }

                if (_head == null || !_head.Completed)
                {
                    EnqueueRequests(new List<AckRequest> { CreateEmptyAckRequest() });
                    return true;
                }

                var handlers = ContinuousCompleteHandlers();
                _beginScanPosition = handlers[handlers.Count - 1].Item2.Next;

                var requests = new List<AckRequest>(handlers.Count);
                foreach (var segment in handlers)
                {
                    var request = new AckRequest
                    {
                        Subject = Subject,
                        Group = Group,
                        IsBroadcast = _isBroadcast,
                        ConsumerId = ClientId.CurrentClientId,
                        PullOffsetBegin = segment.Item1.PullLogOffset,
                        PullOffsetEnd = segment.Item2.PullLogOffset,
                    };
                    requests.Add(request);
                }

                _head = _beginScanPosition;
                // All message is acked, need reset head and tail
                if (_head == null)
                {
                    _tail = null;
                }

                UpdateLastAckTime();
                EnqueueRequests(requests);
                return true;
            }
        }

        private AckRequest CreateEmptyAckRequest()
        {
            return new AckRequest
            {
                Subject = Subject,
                Group = Group,
                IsBroadcast = _isBroadcast,
                ConsumerId = ClientId.CurrentClientId,
                PullOffsetBegin = -1,
                PullOffsetEnd = -1,
            };
        }

        public void HandleAck(AckHandler handler)
        {
            bool needSendRequests = HandleNormalAckRequests(handler);
            if (needSendRequests)
            {
                TriggerAckSender();
            }
        }

        // TODO(keli.wang): when one ack delay and blocks many other acks, can we just make this one a nack?
        private bool HandleNormalAckRequests(AckHandler handler)
        {
            lock (_updateGuard)
            {
                if (handler != _beginScanPosition) return false;

                var handlers = ContinuousCompleteHandlers();
                _beginScanPosition = handlers[handlers.Count - 1].Item2.Next;

                if (handlers.Count == 1)
                {
                    if (!AllowSendAck(handlers[0].Item2))
                    {
                        return false;
                    }
                }

                var requests = new List<AckRequest>(handlers.Count);
                foreach (var segment in handlers)
                {
                    var request = new AckRequest
                    {
                        Subject = Subject,
                        Group = Group,
                        IsBroadcast = _isBroadcast,
                        ConsumerId = ClientId.CurrentClientId,
                        PullOffsetBegin = segment.Item1.PullLogOffset,
                        PullOffsetEnd = segment.Item2.PullLogOffset,
                    };
                    requests.Add(request);
                }

                _head = _beginScanPosition;
                // All message is acked, need reset head and tail
                if (_head == null)
                {
                    _tail = null;
                }

                UpdateLastAckTime();
                EnqueueRequests(requests);
                return true;
            }
        }

        private bool AllowSendAck(AckHandler lastAckedHandler)
        {
            if (lastAckedHandler.Next == null)
            {
                return true;
            }
            return lastAckedHandler.PullLogOffset - _head.PullLogOffset >= MinAckBatch;
        }

        private List<Tuple<AckHandler, AckHandler>> ContinuousCompleteHandlers()
        {
            var handlers = new List<Tuple<AckHandler, AckHandler>>();

            var begin = _head;
            var end = _beginScanPosition;
            while (end.Next != null && end.Next.Completed)
            {
                if (end.PullLogOffset + 1 != end.Next.PullLogOffset)
                {
                    handlers.Add(new Tuple<AckHandler, AckHandler>(begin, end));
                    begin = end.Next;
                }
                end = end.Next;
            }
            handlers.Add(new Tuple<AckHandler, AckHandler>(begin, end));

            return handlers;
        }

        // protected by _updateGuard
        private void UpdateLastAckTime()
        {
            _lastAckTime = DateTime.Now.ToTime();
        }

        // protected by _updateGuard
        private void EnqueueRequests(IEnumerable<AckRequest> requests)
        {
            foreach (var request in requests)
            {
                _ackRequestQueue.Enqueue(request);
            }
        }

        private void TriggerAckSender()
        {
            if (_sendingAck)
            {
                return;
            }

            AckRequest ackRequest;
            lock (_sendingGuard)
            {
                if (_sendingAck)
                {
                    return;
                }

                _ackRequestQueue.TryPeek(out ackRequest);
                if (ackRequest == null)
                {
                    return;
                }

                _sendingAck = true;
            }

            _ackMessageService.Ack(_brokerGroup, ackRequest).ContinueWith(task =>
            {
                var resp = task.Result;

                var shouldDelayAck = false;
                if (resp.IsOk())
                {
                    _ackRequestQueue.TryDequeue(out _);
                    if (!IsEmptyAck(ackRequest))
                    {
                        MinPullOffset = ackRequest.PullOffsetEnd + 1;
                    }
                }
                else
                {
                    switch (resp.Result)
                    {
                        case NoWritableBrokerException ne:
                            shouldDelayAck = true;
                            LOG.Warn($"Qmq.Consume.SendAckFail {ackRequest.Subject}/{ackRequest.Group}/{_brokerGroup}");
                            break;
                        case Exception e:
                            LOG.Error(e, $"send ack failed, message: {resp.ErrorMessage}");
                            break;
                    }
                }

                if (shouldDelayAck)
                {
                    Scheduler.INSTANCE.Schedule(DelayTriggerAckSender, DateTime.Now.AddSeconds(2));
                }
                else
                {
                    _sendingAck = false;
                    TriggerAckSender();
                }

            }, TaskScheduler.Default);
        }

        private void DelayTriggerAckSender()
        {
            _sendingAck = false;
            TriggerAckSender();
        }

        private static bool IsEmptyAck(AckRequest request)
        {
            return request.PullOffsetBegin == -1 && request.PullOffsetEnd == -1;
        }
    }
}