using Qunar.TC.Qmq.Client.Metainfo;

namespace Qunar.TC.Qmq.Client.NewQmq
{
    interface LoadBalance
    {
        BrokerGroup Select(NewQmqClusterInfo cluster);

        void Timeout(BrokerGroup group);

        void NoMessage(BrokerGroup group);

        void FetchedMessages(BrokerGroup group);

        void FetchedEnoughMessages(BrokerGroup group);
    }
}
