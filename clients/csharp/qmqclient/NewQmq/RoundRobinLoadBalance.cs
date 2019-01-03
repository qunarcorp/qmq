using Qunar.TC.Qmq.Client.Metainfo;
using Qunar.TC.Qmq.Client.Util;
using System;
using System.Threading;

namespace Qunar.TC.Qmq.Client.NewQmq
{
    class RoundRobinLoadBalance : LoadBalance
    {
        private int index;

        public BrokerGroup Select(NewQmqClusterInfo cluster)
        {
            if (cluster.BrokerGroups.Count == 0) return null;
            var groups = DictValueFilter.Filter(cluster.BrokerGroups, BrokerGroup.IsReadable);
            if (groups == null || groups.Count == 0) return null;

            var count = groups.Count;

            return groups[Math.Abs(Interlocked.Increment(ref index) % count)];
        }

        public void FetchedEnoughMessages(BrokerGroup group)
        {

        }

        public void FetchedMessages(BrokerGroup group)
        {

        }

        public void NoMessage(BrokerGroup group)
        {

        }



        public void Timeout(BrokerGroup group)
        {

        }
    }
}
