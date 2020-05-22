using System;

namespace Qunar.TC.Qmq.Client.Exceptions
{
    public class NeedRetryException : Exception
    {
        private readonly TimeSpan next;

        public NeedRetryException(TimeSpan next, string message) : base(message)
        {
            this.next = next;
        }

        public TimeSpan Next
        {
            get
            {
                return this.next;
            }
        }
    }
}
