// yuzhaohui
// 2016/8/3
using System;
using System.Net;
using System.Net.Sockets;
namespace Qunar.TC.Qmq.Client.Util
{
    static class NetUtils
    {
        public static string LocalHost()
        {
            var host = Dns.GetHostEntry(Dns.GetHostName());
            foreach (var ip in host.AddressList)
            {
                if (ip.AddressFamily == AddressFamily.InterNetwork)
                {
                    return ip.ToString();
                }
            }
            throw new Exception("Local IP Address Not Found!");
        }
    }
}

