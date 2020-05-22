using Qunar.TC.Qmq.Client.Metainfo;
using Qunar.TC.Qmq.Client.Util;
using System;
using System.Collections.Concurrent;

namespace Qunar.TC.Qmq.Client.NewQmq
{
    class WeightLoadBalance : LoadBalance
    {
        private const int MIN_WEIGHT = 5;

        private const int MAX_WEIGHT = 150;

        private const int DEFAULT_WEIGHT = 100;

        private readonly ConcurrentDictionary<string, int> weights = new ConcurrentDictionary<string, int>();

        public BrokerGroup Select(NewQmqClusterInfo cluster)
        {
            if (cluster.BrokerGroups.Count == 0) return null;
            var groups = DictValueFilter.Filter(cluster.BrokerGroups, BrokerGroup.IsReadable);
            if (groups == null || groups.Count == 0) return null;

            var totalWeight = 0;
            var sameWeight = true;
            var lastWeight = -1;

            foreach (var group in groups)
            {
                if (!weights.TryGetValue(group.Name, out int weight))
                {
                    weights.TryAdd(group.Name, DEFAULT_WEIGHT);
                    weight = DEFAULT_WEIGHT;
                }

                if (lastWeight != -1 && lastWeight != weight)
                {
                    sameWeight = false;
                }
                lastWeight = weight;
                totalWeight += weight;
            }

            if (totalWeight == 0) return null;

            if (totalWeight > 0 && !sameWeight)
            {
                int offset = StaticRandom.NextRand(totalWeight);
                foreach (var group in groups)
                {
                    weights.TryGetValue(group.Name, out int weight);
                    offset -= weight;
                    if (offset <= 0)
                    {
                        return group;
                    }
                }
            }

            var index = StaticRandom.NextRand(groups.Count);
            return groups[index];
        }

        public void Timeout(BrokerGroup group)
        {
            Update(group, 0.25, DEFAULT_WEIGHT);
        }

        public void FetchedEnoughMessages(BrokerGroup group)
        {
            Update(group, 1.5, MAX_WEIGHT);
        }

        public void FetchedMessages(BrokerGroup group)
        {
            Update(group, 1.25, DEFAULT_WEIGHT);
        }

        public void NoMessage(BrokerGroup group)
        {
            Update(group, 0.75, MAX_WEIGHT);
        }

        private void Update(BrokerGroup group, double factor, int maxWeight)
        {
            if (!weights.TryGetValue(group.Name, out int weight))
            {
                weights.TryAdd(group.Name, DEFAULT_WEIGHT);
                weight = DEFAULT_WEIGHT;
            }

            var newWeight = Math.Min(Math.Max((int)(factor * weight), MIN_WEIGHT), maxWeight);
            weights.TryUpdate(group.Name, weight, weight);
        }
    }
}
