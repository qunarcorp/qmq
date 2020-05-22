using System.Collections.Concurrent;

namespace Qunar.TC.Qmq.Client.NewQmq
{
    internal class AckHandlerQueueManager
    {
        private readonly ConcurrentDictionary<string, AckHandlerQueue> _queues = new ConcurrentDictionary<string, AckHandlerQueue>();
        private readonly AckMessageService _ackMessageService;
        private readonly SendMessageBackService _sendMessageBackService;

        public AckHandlerQueueManager(AckMessageService ackMessageService, SendMessageBackService sendMessageBackService)
        {
            _ackMessageService = ackMessageService;
            _sendMessageBackService = sendMessageBackService;
        }

        public AckHandlerQueue GetOrCreate(string subject, string group, string brokerGroup, bool isBroadcast)
        {
            return _queues.GetOrAdd(
                QueueKey(subject, group, brokerGroup),
                _ => new AckHandlerQueue(subject, group, brokerGroup, isBroadcast, _ackMessageService, _sendMessageBackService));
        }

        private static string QueueKey(string subject, string group, string brokerGroup)
        {
            return $"{subject}/{group}/{brokerGroup}";
        }
    }
}