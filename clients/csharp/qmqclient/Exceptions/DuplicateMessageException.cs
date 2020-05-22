namespace Qunar.TC.Qmq.Client.Exceptions
{
    class DuplicateMessageException : MessageException
    {
        public DuplicateMessageException() { }

        public DuplicateMessageException(string messageId) : base(messageId, "Duplicated message")
        {
        }
    }
}

