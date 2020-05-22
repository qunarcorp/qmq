using Qunar.TC.Qmq.Client.Model;

namespace Qunar.TC.Qmq.Client.Consumer
{
    internal interface IMessageHandleTask
    {
        BaseMessage Message();

        object DispatchTo(MessageHandler handler);

        void Execute(MessageListener listener);
    }
}