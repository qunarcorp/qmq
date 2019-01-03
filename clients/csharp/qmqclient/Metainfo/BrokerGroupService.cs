using System;
using System.Collections.Concurrent;
using Qunar.TC.Qmq.Client.Util;

namespace Qunar.TC.Qmq.Client.Metainfo
{
    internal class BrokerGroupService
    {
        private static readonly NLog.Logger Logger = NLog.LogManager.GetCurrentClassLogger();

        private readonly MetainfoClient _client;
        private readonly ConcurrentDictionary<CacheKey, NewQmqClusterInfo> _cache;

        public BrokerGroupService(string appCode, string metaServer)
        {
            _client = new MetainfoClient(appCode, new MetainfoAddressResolver(metaServer));
            _cache = new ConcurrentDictionary<CacheKey, NewQmqClusterInfo>();

            Scheduler.INSTANCE.ScheduleAtFixDelay(RefreshCache, 60);
        }

        private void RefreshCache()
        {
            try
            {
                foreach (var cacheKey in _cache.Keys)
                {
                    ForceLoad(cacheKey.Type, cacheKey.Subject, cacheKey.Group);
                }
            }
            catch (Exception e)
            {
                Logger.Debug(e, "refresh error");
            }
        }

        public NewQmqClusterInfo DelayProducerGetSubjectCluster(string subject, bool foreLoad = false)
        {
            return foreLoad ? ForceLoad(ClientType.DELAY_PRODUCER, subject, "") : GetOrRequest(ClientType.DELAY_PRODUCER, subject, "");
        }

        public NewQmqClusterInfo ProducerGetSubjectCluster(string subject, bool forceLoad = false)
        {
            return forceLoad ? ForceLoad(ClientType.Producer, subject, "") : GetOrRequest(ClientType.Producer, subject, "");
        }

        public NewQmqClusterInfo ConsumerGetSubjectCluster(string subject, string group, bool forceLoad = false)
        {
            return forceLoad ? ForceLoad(ClientType.Consumer, subject, group) : GetOrRequest(ClientType.Consumer, subject, group);
        }

        private NewQmqClusterInfo GetOrRequest(ClientType type, string subject, string group)
        {
            return _cache.GetOrAdd(BuildCacheKey(type, subject, group), (_) => _client.GetClusterInfo(type, subject, group));
        }

        private NewQmqClusterInfo ForceLoad(ClientType type, string subject, string group)
        {
            _client.Refresh(type, subject, group);
            var cluster = _client.GetClusterInfo(type, subject, group);
            if (cluster.IsValid())
            {
                _cache[BuildCacheKey(type, subject, group)] = cluster;
                return cluster;
            }

            return _cache[BuildCacheKey(type, subject, group)];
        }

        private static CacheKey BuildCacheKey(ClientType type, string subject, string group)
        {
            return new CacheKey(type, subject, group);
        }

        private class CacheKey
        {
            public CacheKey(ClientType type, string subject, string group)
            {
                Type = type;
                Subject = subject;
                Group = group;
            }

            public ClientType Type { get; }

            public string Subject { get; }

            public string Group { get; }

            protected bool Equals(CacheKey other)
            {
                return Type == other.Type && string.Equals(Subject, other.Subject);
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != this.GetType()) return false;
                return Equals((CacheKey)obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return ((int)Type * 397) ^ (Subject != null ? Subject.GetHashCode() : 0);
                }
            }
        }
    }
}