// yuzhaohui
// 2016/7/19
using System;

namespace Qunar.TC.Qmq.Client.Model
{
    internal class ConsumerRejectException : Exception
    {
        private string detailMessage;

        public override string Message => detailMessage;

        public ConsumerRejectException(string msg) : base(msg)
        {
            detailMessage = msg;
        }
    }
}

