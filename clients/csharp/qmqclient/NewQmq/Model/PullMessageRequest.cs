namespace Qunar.TC.Qmq.Client.NewQmq.Model
{
    internal class PullMessageRequest
    {
        public string Subject { get; set; }
        public string Group { get; set; }
        public int BatchSize { get; set; }
        public long TimeoutMillis { get; set; }
        public long Offset { get; set; }
        public long PullOffsetBegin { get; set; }
        public long PullOffsetEnd { get; set; }
        public string ConsumerId { get; set; }
        public bool IsBroadcast { get; set; }
        public BrokerGroupInfo BrokerGroupInfo { get; set; }
        public short TagType { get; set; }
        public string[] Tags { get; set; }
    }
}
