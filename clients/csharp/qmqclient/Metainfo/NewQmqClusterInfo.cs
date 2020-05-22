using System.Collections.Generic;

namespace Qunar.TC.Qmq.Client.Metainfo
{
    internal class NewQmqClusterInfo
    {
        private readonly bool _valid;
        private readonly Dictionary<string, BrokerGroup> _brokerGroups = new Dictionary<string, BrokerGroup>();

        internal NewQmqClusterInfo()
        {
            _valid = true;
        }

        internal NewQmqClusterInfo(bool valid)
        {
            _valid = valid;
        }

        public string Subject { get; set; }

        public ClientType ClientType { get; set; }

        public void AddBrokerGroup(BrokerGroup group)
        {
            _brokerGroups.Add(group.Name, group);
        }

        public Dictionary<string, BrokerGroup> BrokerGroups
        {
            get { return _brokerGroups; }
        }

        internal bool IsValid()
        {
            return _valid;
        }
    }

    internal class BrokerGroup
    {
        public string Name { get; set; }

        public string Master { get; set; }

        public string Idc { get; set; }

        public long UpdateTs { get; set; }

        public BrokerState BrokerState { get; set; }

        public static bool IsReadable(BrokerGroup brokerGroup)
        {
            return brokerGroup.BrokerState == BrokerState.R || brokerGroup.BrokerState == BrokerState.Rw;
        }

        public static bool IsWritable(BrokerGroup brokerGroup)
        {
            return brokerGroup.BrokerState == BrokerState.W || brokerGroup.BrokerState == BrokerState.Rw;
        }
    }

    internal enum BrokerState
    {
        Rw = 1, R, W, Nrw
    }

    public enum ClientType
    {
        Producer = 1,
        Consumer = 2,
        OTHER = 3,
        DELAY_PRODUCER = 4
    }
}
