// yuzhaohui
// 2016/7/15

using System;
using Qunar.TC.Qmq.Client.Consumer;
using Qunar.TC.Qmq.Client.tag;

namespace Qunar.TC.Qmq.Client
{
    internal interface ListenerHolder
    {
        void Listen(ExtraListenerConfig config);

        void Stop();
    }

    internal class ExtraListenerConfig
    {
        public ExtraListenerConfig(ConsumeMode mode, int pullBatchSize, TimeSpan pullBatchTimeout, TagType tagType, string[] tags)
        {
            Mode = mode;
            PullBatchSize = pullBatchSize;
            PullBatchTimeout = pullBatchTimeout;
            TagType = tagType;
            Tags = tags;
        }

        public ConsumeMode Mode { get; }

        public int PullBatchSize { get; }

        public TimeSpan PullBatchTimeout { get; }

        public TagType TagType { get; }

        public string[] Tags { get; }
    }
}
