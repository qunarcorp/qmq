namespace Qunar.TC.Qmq.Client.Exceptions
{
    class EnqueueException : MessageException
    {
        public EnqueueException() { }

        public EnqueueException(string messageId) : base(messageId, "message enqueue fail")
        {
        }
    }
}
