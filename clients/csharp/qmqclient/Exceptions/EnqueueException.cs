using Qunar.TC.Qmq.Client.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Qunar.TC.Qmq.Client.Exceptions
{
    class EnqueueException : MessageException
    {
        public EnqueueException() { }

        public EnqueueException(string messageId) : base(messageId, "message enqueue fail")
        {
        }
    }
}
