namespace Qunar.TC.Qmq.Client.Model
{
    internal class SubscribeRequest
    {
        private readonly string consumer;
        private readonly string prefix;
        private readonly string group;

        public SubscribeRequest(string consumer, string prefix, string group)
        {
            this.consumer = consumer;
            this.prefix = prefix;
            this.group = group;
        }

        public string Consumer
        {
            get { return consumer; }
        }

        public string Prefix
        {
            get { return prefix; }
        }

        public string Group
        {
            get { return group; }
        }

        public override bool Equals(object obj)
        {
            if (obj == null) return false;
            var other = obj as SubscribeRequest;
            if (other == null) return false;
            if (this == other) return true;
            return consumer.Equals(other.consumer);
        }

        public override int GetHashCode()
        {
            return consumer.GetHashCode();
        }
    }
}
