using System;
using System.Net;
using System.Net.NetworkInformation;

namespace Qunar.TC.Qmq.Client.Util
{
    internal class PortSelector
    {
        public static int Select()
        {
            //int processId = Process.GetCurrentProcess().Id;
            //int start = Math.Max(processId, 9000);
            //return new Random(start).Next(65535);

            Random random = new Random(DateTime.Now.Millisecond);
            int port = random.Next(9000, 65535);

            if (!PortInUse(port))
            {
                return port;
            }
            else
            {
                return Select();
            }
        }

        internal static bool PortInUse(int port)
        {
            bool inUse = false;

            IPGlobalProperties ipProperties = IPGlobalProperties.GetIPGlobalProperties();
            IPEndPoint[] ipEndPoints = ipProperties.GetActiveTcpListeners();

            foreach (IPEndPoint endPoint in ipEndPoints)
            {
                if (endPoint.Port == port)
                {
                    inUse = true;
                    break;
                }
            }


            return inUse;
        }
    }
}
