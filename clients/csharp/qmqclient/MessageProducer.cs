using System;

namespace Qunar.TC.Qmq.Client
{
    /// <summary>
    /// QMQ生产者接口
    /// </summary>
	public interface MessageProducer
    {
        /// <summary>
        /// 生成消息
        /// </summary>
        /// <param name="subject">指定subject</param>
        /// <returns></returns>
		Message GenerateMessage(string subject);

        /// <summary>
        /// 发送消息
        /// 
        /// 可以使用onSuccess和onFailed回调监控消息发送状态，但是需要注意的是，如果使用的是事务或持久消息，
        /// onFailed触发也并不意味着消息最后丢失
        /// </summary>
        /// <param name="message">使用GenerateMessage生成的消息，并设置好业务属性</param>
        /// <param name="onSuccess">该消息成功发送到broker时会调用，可选</param>
        /// <param name="onFailed">该消息发送到broker失败时会调用，可选</param>
		void Send(Message message, Action<Message> onSuccess = null, Action<Message> onFailed = null);
    }
}

