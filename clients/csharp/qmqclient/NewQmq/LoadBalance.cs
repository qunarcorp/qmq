using Qunar.TC.Qmq.Client.Metainfo;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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
