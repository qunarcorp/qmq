using System;
using System.Collections.Generic;

namespace Qunar.TC.Qmq.Client.NewQmq.Model
{
    public class BrokerGroupInfo
    {
        private int groupIndex;
        private String groupName;
        private string master;
        private List<String> slaves;
        private volatile bool available = true;

        public BrokerGroupInfo(int groupIndex, string groupName, string master, List<string> slaves, bool available)
        {
            this.GroupIndex = groupIndex;
            this.GroupName = groupName;
            this.Master = master;
            this.Slaves = slaves;
        }

        public int GroupIndex { get => groupIndex; set => groupIndex = value; }
        public string GroupName { get => groupName; set => groupName = value; }
        public string Master { get => master; set => master = value; }
        public List<string> Slaves { get => slaves; set => slaves = value; }
        public bool Available { get => available; set => available = value; }
    }
}
