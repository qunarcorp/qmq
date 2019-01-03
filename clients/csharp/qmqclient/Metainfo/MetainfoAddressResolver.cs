using Qunar.TC.Qmq.Client.Util;
using System;
using System.IO;
using System.Net;
using System.Text;

namespace Qunar.TC.Qmq.Client.Metainfo
{
    internal class MetainfoAddressResolver
    {
        private static readonly NLog.Logger logger = NLog.LogManager.GetCurrentClassLogger();

        private readonly string _metaServer;
        private readonly WaitHolder<HostAndPort> hostAndPortFuture = new WaitHolder<HostAndPort>();
        private long lastUpdateTs;

        public MetainfoAddressResolver(string metaServer)
        {
            _metaServer = metaServer;
        }

        public HostAndPort Resolve()
        {
            Request();
            return hostAndPortFuture.Result;
        }

        private void Refresh()
        {
            try
            {
                var request = (HttpWebRequest)WebRequest.Create(_metaServer);
                request.Timeout = 20000;
                request.Method = "GET";

                request.BeginGetResponse((ar) =>
                {
                    try
                    {
                        using (var response = (HttpWebResponse)request.EndGetResponse(ar))
                        {
                            var statusCode = response.StatusCode;
                            if (statusCode != HttpStatusCode.OK)
                            {
                                return;
                            }
                            using (var responseStream = response.GetResponseStream())
                            {
                                using (var streamReader = new StreamReader(responseStream, Encoding.UTF8))
                                {
                                    var content = streamReader.ReadToEnd();
                                    var arr = content.Split(':');
                                    hostAndPortFuture.Result = new HostAndPort(arr[0], int.Parse(arr[1]));
                                    lastUpdateTs = DateTime.Now.ToTime();
                                }
                            }
                        }
                    }
                    catch { }
                }, null);
            }
            catch (Exception e)
            {
                logger.Warn(e, "refresh metaserver address failed");
            }

        }

        private void Request()
        {
            if (DateTime.Now.ToTime() - lastUpdateTs < 1000)
            {
                return;
            }

            try
            {
                Refresh();
            }
            finally
            {
                Scheduler.INSTANCE.Schedule(Request, DateTime.Now.AddSeconds(20));
            }
        }
    }
}
