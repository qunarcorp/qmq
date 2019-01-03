using System;

namespace Qunar.TC.Qmq.Client.Exceptions
{
    internal class BrokerUnassignedException : Exception
    {
        internal BrokerUnassignedException(string subject)
            : base($"qmq metaserver doesn't assign any broker for {subject} now")
        {
        }

        internal BrokerUnassignedException(string subject, string group)
            : base($"qmq metaserver doesn't assign any broker for {subject}/{group} now")
        {
        }
    }
}