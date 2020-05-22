using System;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using NLog;

namespace Qunar.TC.Qmq.Client.Util
{
    internal static class ClientId
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();
        private static readonly ThreadLocal<MD5> ThreadLocalMd5 = new ThreadLocal<MD5>(MD5.Create);

        private static readonly string HostName = System.Net.Dns.GetHostName();
        private static readonly string UniqueId = CreateUniqueId();

        static ClientId()
        {
            Logger.Info($"qmq client id init done. hostname: {HostName}, unique id: {UniqueId}");
        }

        private static string CreateUniqueId()
        {
            var appPoolName = GetAppPoolName();
            if (!string.IsNullOrEmpty(appPoolName))
            {
                return GetMd5Hash(ThreadLocalMd5.Value, appPoolName);
            }

            var deployPath = AppDomain.CurrentDomain.BaseDirectory;
            return string.IsNullOrEmpty(deployPath) ? Guid.NewGuid().ToString() : GetMd5Hash(ThreadLocalMd5.Value, deployPath);
        }

        private static string GetAppPoolName()
        {
            //var siteName = HostingEnvironment.
            //var virtualPath = HostingEnvironment.ApplicationVirtualPath;
            //if (string.IsNullOrEmpty(siteName) || string.IsNullOrEmpty(virtualPath))
            //{
            //    return "";
            //}
            //else
            //{
            //    return $"{siteName}/{virtualPath}";
            //}
            return "";
        }

        private static string GetMd5Hash(MD5 md5Hash, string input)
        {
            var data = md5Hash.ComputeHash(Encoding.UTF8.GetBytes(input));
            var sb = new StringBuilder();
            foreach (var t in data)
            {
                sb.Append(t.ToString("x2"));
            }
            return sb.ToString();
        }

        public static string CurrentClientId { get; } = $"{HostName}@@{UniqueId}";

        public static string CurrentUniqeConsumerGroup { get; } = $"{HostName}:{UniqueId}";
    }
}