using System;
using Qunar.TC.Qmq.Client.Metainfo;

namespace Qunar.TC.Qmq.Client.NewQmq
{
    internal class PullController
    {
        public PullController(string appCode, string metaServer)
        {
            BrokerGroupService = new BrokerGroupService(appCode, metaServer);
            var sendMessageBackService = new SendMessageBackService(BrokerGroupService);
            AckMessageService = new AckMessageService(BrokerGroupService);
            AckHandlerQueueManager = new AckHandlerQueueManager(AckMessageService, sendMessageBackService);
            PullMessageService = new PullMessageService(AckHandlerQueueManager);
        }

        public PullMessageService PullMessageService { get; }

        public AckMessageService AckMessageService { get; }

        public AckHandlerQueueManager AckHandlerQueueManager { get; }

        public BrokerGroupService BrokerGroupService { get; }
    }
}