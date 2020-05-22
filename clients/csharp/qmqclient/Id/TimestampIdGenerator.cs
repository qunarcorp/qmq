using System;
using System.Text;
using System.Diagnostics;
using System.Threading;

using Qunar.TC.Qmq.Client.Util;

namespace Qunar.TC.Qmq.Client
{
    class TimestampIdGenerator : IdGenerator
    {
        private static int index;

        private static readonly string IP = NetUtils.LocalHost();

        private static readonly int PID = Process.GetCurrentProcess().Id;

        public string Generate()
        {
            //time
            var result = new StringBuilder(DateTime.Now.ToString("yyMMdd.HHmmss"));
            //ip
            result.Append('.').Append(IP);

            //pid
            result.Append('.').Append(PID);

            //index
            var order = Interlocked.Increment(ref index) % 100000;
            result.Append(".").Append(order);
            return result.ToString();
        }
    }
}

