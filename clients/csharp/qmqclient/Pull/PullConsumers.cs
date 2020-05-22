using System;
using System.Collections.Generic;
using Qunar.TC.Qmq.Client.Consumer;
using Qunar.TC.Qmq.Client.tag;

namespace Qunar.TC.Qmq.Client.Pull
{
    public static class PullConsumers
    {
        private static readonly object CacheGuard = new object();
        private static readonly Dictionary<string, IPullConsumer> ConsumerCache = new Dictionary<string, IPullConsumer>();

        public static IPullConsumer DefaultConsumer(string subject, string group, ConsumeMode mode, string appCode, string metaServer)
        {
            return DefaultConsumer(subject, group, mode, appCode, metaServer, TagType.OR, null);
        }

        internal static IPullConsumer DefaultConsumer(string subject, string group, ConsumeMode mode, string appCode, string metaServer, TagType tagType, string[] tags)
        {
            if (string.IsNullOrEmpty(subject))
            {
                throw new ArgumentException("subject不能为空");
            }
            if (string.IsNullOrEmpty(group))
            {
                throw new ArgumentException("group不能为空");
            }

            lock (CacheGuard)
            {
                var key = BuildConsumerKey(subject, group);
                if (ConsumerCache.TryGetValue(key, out var consumer))
                {
                    if (consumer.ConsumeMode == mode)
                    {
                        return consumer;
                    }
                    else
                    {
                        throw new ArgumentException($"已经以{consumer.ConsumeMode}模式创建了消费者，不允许同一消费族使用两种不同的模式消费同一主题");
                    }
                }
                else
                {
                    var newConsumer = new DefaultPullConsumer(subject, group, mode, false, appCode, metaServer, tagType, tags);
                    ConsumerCache.Add(key, newConsumer);
                    return newConsumer;
                }
            }
        }

        public static IPullConsumer BroadcastConsumer(string subject, ConsumeMode mode, string appCode, string metaServer)
        {
            if (string.IsNullOrEmpty(subject))
            {
                throw new ArgumentException("subject不能为空");
            }

            lock (CacheGuard)
            {
                var key = BuildConsumerKey(subject, "");
                if (ConsumerCache.TryGetValue(key, out var consumer))
                {
                    if (consumer.ConsumeMode == mode)
                    {
                        return consumer;
                    }
                    else
                    {
                        throw new ArgumentException($"已经以{consumer.ConsumeMode}模式创建了消费者，不允许同一消费族使用两种不同的模式消费同一主题");
                    }
                }
                else
                {
                    var newConsumer = new DefaultPullConsumer(subject, "", mode, true, appCode, metaServer, TagType.OR, null);
                    ConsumerCache.Add(key, newConsumer);
                    return newConsumer;
                }
            }
        }

        private static string BuildConsumerKey(string subject, string group)
        {
            return $"{subject}/{group}";
        }
    }
}