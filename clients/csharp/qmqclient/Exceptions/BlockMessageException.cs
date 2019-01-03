namespace Qunar.TC.Qmq.Client.Exceptions
{
    internal class BlockMessageException : MessageException
    {
        public BlockMessageException() { }

        public BlockMessageException(string messageId)
            : base(messageId, "block by whitelist")
        {

        }
    }
}
