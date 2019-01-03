namespace Qunar.TC.Qmq.Client.NewQmq.Model
{
    internal class AckRequest
    {
        public string Subject { get; set; }
        public string Group { get; set; }
        public string ConsumerId { get; set; }

        public long PullOffsetBegin { get; set; }
        public long PullOffsetEnd { get; set; }

        public bool IsBroadcast { get; set; }
    }
}
