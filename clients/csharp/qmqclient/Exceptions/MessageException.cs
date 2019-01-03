using System;

//qunar.tc.qmq.service.exceptions.MessageException
namespace Qunar.TC.Qmq.Client.Exceptions
{
    internal class MessageException : Exception
    {
        public const string RejectMessage = "message rejected";
        public const string UnkonwMessage = "unkonwn exception";
        public const string BrokerBusy = "broker busy";

        private string messageId;
        private string detailMessage;

        public MessageException()
        {
        }

        public MessageException(string messageId, string msg, Exception t)
            : base(msg, t)
        {

            this.detailMessage = msg;
            this.messageId = messageId;
        }

        public MessageException(string messageId, string msg) : this(messageId, msg, null)
        {
        }

        public string MessageId
        {
            get
            {
                return this.messageId;
            }
        }

        public override string Message
        {
            get { return detailMessage; }
        }

        public bool IsRejected()
        {
            return RejectMessage.Equals(Message);
        }

        public bool IsBrokerBusy()
        {
            return BrokerBusy.Equals(Message);
        }
    }
}

