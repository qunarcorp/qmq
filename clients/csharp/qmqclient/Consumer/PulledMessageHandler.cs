namespace Qunar.TC.Qmq.Client.Consumer
{
    internal class PulledMessageHandler
    {
        public IMessageHandleTask CreateTask(PulledMessage message)
        {
            return new PulledMessageHandleTask(message);
        }
    }
}