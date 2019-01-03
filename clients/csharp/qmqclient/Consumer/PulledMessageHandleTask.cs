using System;
using Qunar.TC.Qmq.Client.Model;
using Qunar.TC.Qmq.Client.Exceptions;
using Qunar.TC.Qmq.Client.Util;

namespace Qunar.TC.Qmq.Client.Consumer
{
    internal class PulledMessageHandleTask : IMessageHandleTask
    {
        private static readonly NLog.Logger Logger = NLog.LogManager.GetCurrentClassLogger();

        private readonly PulledMessage _message;

        public PulledMessageHandleTask(PulledMessage message)
        {
            _message = message;
        }

        public BaseMessage Message()
        {
            return _message;
        }

        public object DispatchTo(MessageHandler handler)
        {
            if (handler.Handle(this))
            {
                return new ChannelState(0, 0);
            }

            return new ConsumerRejectException("客户端线程已满: 0");
        }

        public void Execute(MessageListener listener)
        {
            var start = DateTime.Now.ToTime();
            _message.SetStart(start);
            Exception ex = null;
            Logger.Info($"process message: {_message.Subject}/{_message.MessageId}");
            try
            {
                listener(_message);
            }
            catch (Exception e)
            {
                ex = e;
                if (!(e is NeedRetryException))
                {
                    Logger.Error(e, $"process msg error: {_message.MessageId}");
                }
            }
            finally
            {
                if (_message.AutoAck || ex != null)
                {
                    _message.Ack(DateTime.Now.ToTime() - start, ex);
                }
            }
        }
    }
}