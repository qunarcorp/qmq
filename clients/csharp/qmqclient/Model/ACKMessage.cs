// yuzhaohui
// 2016/7/19
namespace Qunar.TC.Qmq.Client.Model
{
    class ACKMessage
    {
        private readonly string messageID;
        private readonly string prefix;
        private readonly string group;
        private readonly long elapsed;
        private readonly string errorMessage;
        private long next;

        public ACKMessage(string messageID, string prefix, string group, long elapsed, string errorMessage, long next)
        {
            this.messageID = messageID;
            this.prefix = prefix;
            this.group = group;
            this.elapsed = elapsed;
            this.errorMessage = errorMessage;
            this.next = next;
        }

        public override string ToString()
        {
            return "ACKMessage [messageID=" + this.messageID + ", prefix=" + this.prefix + ", group=" + this.group + ", elapsed=" + this.elapsed + ", errorMessage=" + this.errorMessage + "]";
        }
    }
}

