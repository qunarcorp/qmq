using System;
using System.Collections.Generic;
using Qunar.TC.Qmq.Client.Consumer;

namespace Qunar.TC.Qmq.Client.Pull
{
    public interface IPullConsumer
    {
        string Subject { get; }

        string ConsumerGroup { get; }

        bool IsBroadcast { get; }

        ConsumeMode ConsumeMode { get; }

        /// <summary>
        /// 纯拉api
        /// 注意：使用该api拉到消息使用方需要对每条消息调用message.Ack方法，如果不care消息是否消费成功，可以设置ConsumeMode = ConsumeMode.AtMostOnce
        /// 这样就不用显式调用Ack方法了
        /// </summary>
        /// <param name="expectNum">期望本次最多拉到多少条消息</param>
        /// <param name="timeout">在server端暂时没有消息时希望等待的超时时间</param>
        /// <returns></returns>
        List<Message> Pull(int expectNum, TimeSpan timeout);
    }
}