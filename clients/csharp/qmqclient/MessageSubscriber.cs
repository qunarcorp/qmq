// yuzhaohui
// 2016/8/8

using System;
using Qunar.TC.Qmq.Client.Consumer;
using Qunar.TC.Qmq.Client.tag;

namespace Qunar.TC.Qmq.Client
{
    public interface MessageSubscriber
    {
        event MessageListener Received;

        ConsumeMode Mode
        {
            get;
            set;
        }

        int PullBatchSize
        {
            get;
            set;
        }

        TimeSpan PullBatchTimeout
        {
            get;
            set;
        }

        string[] Tags
        {
            set;
        }

        TagType TagType
        {
            set;
        }

        void Start();

        void Stop();
    }
}
