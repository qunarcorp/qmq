using Qunar.TC.Qmq.Client.Codec;
using Qunar.TC.Qmq.Client.Consumer;
using Qunar.TC.Qmq.Client.Exceptions;
using Qunar.TC.Qmq.Client.Metainfo;
using Qunar.TC.Qmq.Client.Model;
using Qunar.TC.Qmq.Client.NewQmq;
using Qunar.TC.Qmq.Client.NewQmq.Model;
using Qunar.TC.Qmq.Client.tag;
using Qunar.TC.Qmq.Client.Util;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Qunar.TC.Qmq.Client.Pull
{
    class Puller
    {
        private readonly string _subject;
        private readonly string _originSubject;
        private readonly string _consumerGroup;
        private readonly bool _isBroadcast;
        private readonly short _tagType;
        private readonly string[] _tags;
        private readonly ConsumeMode _consumeMode;
        private readonly PullController _controller;

        private readonly PullStrategy _pullStrategy;
        private readonly LoadBalance _loadBalance;

        public Puller(string subject, string consumerGroup, bool isBroadcast, TagType tagType, string[] tags, ConsumeMode consumeMode, PullController controller, PullStrategy pullStrategy, LoadBalance loadBalance)
        {
            _subject = subject;
            _originSubject = RetrySubjectUtils.RealSubject(_subject);
            _consumerGroup = consumerGroup;
            _isBroadcast = isBroadcast;
            _consumeMode = consumeMode;
            _tagType = (_tags == null || _tags.Length == 0) ? (byte)0 : (byte)tagType;
            _tags = tags;

            _controller = controller;

            _pullStrategy = pullStrategy;
            _loadBalance = loadBalance;
        }

        public Task<PullResult> Pull(int expectNum, TimeSpan timeout)
        {
            if (!_pullStrategy.NeedPull())
            {
                return SkipPull();
            }

            var cluster = _controller.BrokerGroupService.ConsumerGetSubjectCluster(_originSubject, _consumerGroup);
            if (cluster.BrokerGroups.Count == 0)
            {
                return ErrorTask(new BrokerUnassignedException(_subject, _consumerGroup));
            }

            var brokerGroup = _loadBalance.Select(cluster);
            if (brokerGroup == null)
            {
                return ErrorTask(new NoWritableBrokerException($"{_subject}/{_consumerGroup}"));
            }

            var request = CreatePullRequest(expectNum, timeout);
            return _controller.PullMessageService.Pull(brokerGroup, request).ContinueWith(task =>
            {
                var response = task.Result;

                RefreshMetaOnDemand(response);
                AdjustLoadBalance(brokerGroup, expectNum, response);

                if (response.IsOk())
                {
                    return PullResult.Ok(RegisterAckHandler(_subject, brokerGroup.Name, (List<BaseMessage>)response.Result));
                }

                if (response.Result is Exception ex)
                {
                    return PullResult.Err(ex, response.ErrorMessage);
                }

                return PullResult.Ok(new List<Message>());
            }, TaskScheduler.Default).ContinueWith(task =>
            {
                // 请求成功的加权，失败的降权，避免没有消息的主题一直空拉阻塞降低效率
                _pullStrategy.Record(task.Result.Success && task.Result.Messages.Count != 0);
                return task.Result;
            });
        }

        private void RefreshMetaOnDemand(Response response)
        {
            if (response.Result is NoWritableBrokerException)
            {
                _controller.BrokerGroupService.ConsumerGetSubjectCluster(_subject, _consumerGroup, true);
            }
        }

        private void AdjustLoadBalance(BrokerGroup group, int expectedNum, Response response)
        {
            if (!response.IsOk())
            {
                _loadBalance.Timeout(group);
                return;
            }

            var result = response.Result;
            if (result is Exception)
            {
                _loadBalance.Timeout(group);
                return;
            }

            var messages = (List<BaseMessage>)result;
            if (messages == null || messages.Count == 0)
            {
                _loadBalance.NoMessage(group);
                return;
            }

            if (messages.Count >= expectedNum)
            {
                _loadBalance.FetchedEnoughMessages(group);
                return;
            }

            _loadBalance.FetchedMessages(group);
        }

        private List<Message> RegisterAckHandler(string subject, string brokerGroupName, IList<BaseMessage> messages)
        {
            if (messages.Count == 0)
            {
                return new List<Message>();
            }

            var handlerQueue = _controller.AckHandlerQueueManager.GetOrCreate(subject, _consumerGroup, brokerGroupName, _isBroadcast);
            var handlers = handlerQueue.AcquireHandlers(messages);

            var wrappedMessages = new List<Message>(messages.Count);
            wrappedMessages.AddRange(messages.Select((t, i) => new PulledMessage(t, handlers[i])).Where((p) =>
            {
                var isCorrupt = p.GetStringProperty(BaseMessage.keys.qmq_corruptData);
                if (!string.IsNullOrEmpty(isCorrupt))
                {
                    p.Ack(0);
                    return false;
                }
                var groupFilter = p.GetStringProperty(BaseMessage.keys.qmq_consumerGroupName);
                if (groupFilter == null)
                {
                    p.SetPropertyForInternal(BaseMessage.keys.qmq_consumerGroupName.ToString(), _consumerGroup);
                    return true;
                }

                if (groupFilter.Equals(_consumerGroup)) return true;
                p.Ack(0);
                return false;
            }));
            // AtMostOnce consumer mode means ack when received
            if (_consumeMode == ConsumeMode.AtMostOnce)
            {
                foreach (var wrappedMessage in wrappedMessages)
                {
                    wrappedMessage.Ack(0);
                }
            }
            return wrappedMessages;
        }

        private static Task<PullResult> SkipPull()
        {
            var tsc = new TaskCompletionSource<PullResult>();
            tsc.TrySetResult(PullResult.Ok(new List<Message>()));
            return tsc.Task;
        }

        private static Task<PullResult> ErrorTask(Exception e)
        {
            var tsc = new TaskCompletionSource<PullResult>();
            tsc.TrySetResult(PullResult.Err(e, "error"));
            return tsc.Task;
        }

        private PullMessageRequest CreatePullRequest(int expectNum, TimeSpan timeout)
        {
            return new PullMessageRequest
            {
                Subject = _subject,
                Group = _consumerGroup,
                BatchSize = expectNum,
                TimeoutMillis = timeout == TimeSpan.MinValue ? -1 : (long)timeout.TotalMilliseconds,
                Offset = -1,
                PullOffsetBegin = 0,
                PullOffsetEnd = 0,
                ConsumerId = ClientId.CurrentClientId,
                IsBroadcast = _isBroadcast,
                TagType = _tagType,
                Tags = _tags
            };
        }
    }
}
