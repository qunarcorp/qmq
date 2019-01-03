using Qunar.TC.Qmq.Client.Codec;
using Qunar.TC.Qmq.Client.Codec.NewQmq;
using Qunar.TC.Qmq.Client.Transport;
using Qunar.TC.Qmq.Client.Util;
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Text;

namespace Qunar.TC.Qmq.Client.Metainfo
{
    internal class MetainfoClient : Processor
    {
        private static readonly TimeSpan MinRefreshInterval = TimeSpan.FromMilliseconds(1000);

        private readonly ConcurrentDictionary<string, WaitHolder<NewQmqClusterInfo>> _clusterMap = new ConcurrentDictionary<string, WaitHolder<NewQmqClusterInfo>>();
        private readonly ConcurrentDictionary<string, DateTime> _lastRefreshTime = new ConcurrentDictionary<string, DateTime>();

        private readonly MetainfoAddressResolver _addressResolver;
        private readonly byte[] _appCode;

        private volatile TransportClient _client;

        private readonly object lockHelper = new object();

        public MetainfoClient(string appCode, MetainfoAddressResolver addressResolver)
        {
            _addressResolver = addressResolver;
            _appCode = Encoding.UTF8.GetBytes(appCode);
        }

        public NewQmqClusterInfo GetClusterInfo(ClientType clientType, string subject, string group)
        {
            var key = clientType + "$" + subject;
            _clusterMap.TryGetValue(key, out var clusterInfoFuture);

            if (clusterInfoFuture != null) return clusterInfoFuture.Result;

            var temp = new WaitHolder<NewQmqClusterInfo>();
            clusterInfoFuture = _clusterMap.GetOrAdd(key, temp);
            if (temp == clusterInfoFuture)
            {
                Refresh(clientType, subject, @group);
            }
            return clusterInfoFuture.Result;
        }

        public void Refresh(ClientType clientType, string subject, string group)
        {
            var key = clientType + "$" + subject;
            if (!CanRefresh(key))
            {
                return;
            }
            _lastRefreshTime[key] = DateTime.Now;

            lock (lockHelper)
            {
                if (_client == null || _client.IsClose)
                {
                    _client?.Close();

                    var hostAndPort = _addressResolver.Resolve();
                    _client = new TransportClient(hostAndPort.Host, hostAndPort.Port, new NewQmqEncoder(), new NewQmqDecoder(), new MetaInfoKeepliveStrategy(), false, 1024)
                    {
                        Processor = this
                    };
                }
            }

            if (!_client.Writtable)
            {
                _client.StateChanged += (sender, e) =>
                {
                    if (e.State == State.Connected)
                    {
                        SendRequest(clientType, subject, group, RequestType.Online);
                    }
                };
                _client.Connect();
            }
            else
            {
                SendRequest(clientType, subject, group);
            }

            Scheduler.INSTANCE.Schedule(() =>
            {
                _clusterMap.TryGetValue(key, out var clusterInfoFuture);
                if (clusterInfoFuture == null || clusterInfoFuture.AlreadySet) return;

                var emptyClusterInfo = new NewQmqClusterInfo(false)
                {
                    Subject = subject,
                    ClientType = clientType
                };
                clusterInfoFuture.Result = emptyClusterInfo;
            }, DateTime.Now.AddSeconds(5));
        }

        private bool CanRefresh(string key)
        {
            if (_lastRefreshTime.TryGetValue(key, out var lastRrefresh))
            {
                return DateTime.Now - lastRrefresh > MinRefreshInterval;
            }

            return true;
        }

        private void SendRequest(ClientType clientType, string subject, string group, RequestType requestType = RequestType.Heartbeat)
        {
            var datagram = new Datagram();
            var remotingHeader = new RemotingHeader
            {
                Version = RemotingHeader.Version8,
                Code = 35,
                Flag = 0,
                Opaque = 0
            };
            datagram.Header = remotingHeader;
            var request = new MetaInfoRequest
            {
                Subject = subject,
                ConsumerGroup = group,
                AppCode = _appCode,
                ClientType = (int)clientType,
                RequestType = (int)requestType,
                ClientId = ClientId.CurrentClientId
            };
            datagram.PayloadHolder = new MetaInfoRequestPayloadHolder(request);
            _client.Send(datagram);

        }

        void Processor.Process(HandlerContext context, object msg)
        {
            if (!(msg is Datagram datagram)) return;

            if (datagram.Header.Code != 0) return;

            var packet = new Packet(datagram.Body, 0, datagram.Body.Length);
            //skip ts
            packet.ReadInt64();
            var subject = packet.ReadUTF8();
            //skip consumerGroup
            packet.ReadUTF8();
            //skip online offline state
            packet.ReadByte();
            int clientTypeCode = packet.ReadByte();

            var clusterInfo = new NewQmqClusterInfo
            {
                Subject = subject,
                ClientType = (ClientType)clientTypeCode
            };

            var brokerCount = packet.ReadInt16();
            for (var i = 0; i < brokerCount; ++i)
            {
                var groupName = packet.ReadUTF8();
                var master = packet.ReadUTF8();
                var ts = packet.ReadInt64();
                var state = packet.ReadByte();

                var group = new BrokerGroup
                {
                    Name = groupName,
                    Master = master,
                    UpdateTs = ts,
                    BrokerState = (BrokerState)state
                };

                clusterInfo.AddBrokerGroup(@group);
            }

            _clusterMap.TryGetValue(clusterInfo.ClientType.ToString() + '$' + clusterInfo.Subject, out var clusterInfoFuture);
            if (clusterInfoFuture != null)
            {
                clusterInfoFuture.Result = clusterInfo;
            }
        }

        private class MetaInfoKeepliveStrategy : IKeepliveStrategy
        {
            private const long WRITE_INTERVAL = 2 * 60 * 1000;
            private const long READ_TIEOUT = 4 * 60 * 1000;

            public void CheckStatus(TransportClient client, long lastWrite, long lastRead, long lastConnect)
            {
                var now = DateTime.Now.ToTime();
                if ((now - lastWrite < WRITE_INTERVAL) && (now - lastRead > READ_TIEOUT))
                {
                    client.Close();
                    client.CloseChannel();
                }
            }
        }

        private enum RequestType
        {
            Online = 1,
            Heartbeat = 2
        }

        private class MetaInfoRequest
        {
            public string Subject { get; set; }

            public int ClientType { get; set; }

            public byte[] AppCode { get; set; }

            public string ClientId { get; set; }

            public string ConsumerGroup { get; set; }

            public int RequestType { get; set; }
        }

        private class MetaInfoRequestPayloadHolder : PayloadHolder
        {
            private readonly MetaInfoRequest _request;

            private static readonly byte[] Subject = Encoding.UTF8.GetBytes("subject");
            private static readonly byte[] SubjectLen = new byte[2];

            private static readonly byte[] ClientTypeCode = Encoding.UTF8.GetBytes("clientTypeCode");
            private static readonly byte[] ClientTypeCodeLen = new byte[2];

            private static readonly byte[] AppCode = Encoding.UTF8.GetBytes("appCode");
            private static readonly byte[] AppCodeLen = new byte[2];

            private static readonly byte[] ClientId = Encoding.UTF8.GetBytes("clientId");
            private static readonly byte[] ClientIdLen = new byte[2];

            private static readonly byte[] ConsumerGroup = Encoding.UTF8.GetBytes("consumerGroup");
            private static readonly byte[] ConsumerGroupLen = new byte[2];

            private static readonly byte[] RequestType = Encoding.UTF8.GetBytes("requestType");
            private static readonly byte[] RequestTypeLen = new byte[2];

            static MetaInfoRequestPayloadHolder()
            {
                Bits.WriteInt16((short)Subject.Length, SubjectLen, 0);
                Bits.WriteInt16((short)ClientTypeCode.Length, ClientTypeCodeLen, 0);
                Bits.WriteInt16((short)AppCode.Length, AppCodeLen, 0);
                Bits.WriteInt16((short)ClientId.Length, ClientIdLen, 0);
                Bits.WriteInt16((short)ConsumerGroup.Length, ConsumerGroupLen, 0);
                Bits.WriteInt16((short)RequestType.Length, RequestTypeLen, 0);
            }

            private static byte[] InitLenBuffer(byte[] buffer, byte[] lenBuffer)
            {
                Bits.WriteInt16((short)buffer.Length, lenBuffer, 0);
                return lenBuffer;
            }


            public MetaInfoRequestPayloadHolder(MetaInfoRequest request)
            {
                this._request = request;
            }

            public void Write(Stream output)
            {
                var start = output.Position;
                output.Position += 2;

                short size = 4;

                byte[] lenBuffer = new byte[2];

                output.Write(SubjectLen, 0, 2);
                output.Write(Subject, 0, Subject.Length);
                ByteBufHelper.WriteString(_request.Subject, lenBuffer, output);

                output.Write(ClientTypeCodeLen, 0, 2);
                output.Write(ClientTypeCode, 0, ClientTypeCode.Length);
                ByteBufHelper.WriteString(_request.ClientType.ToString(), lenBuffer, output);

                output.Write(AppCodeLen, 0, 2);
                output.Write(AppCode, 0, AppCode.Length);
                ByteBufHelper.WriteInt16((short)_request.AppCode.Length, lenBuffer, output);
                output.Write(_request.AppCode, 0, _request.AppCode.Length);

                if (!string.IsNullOrEmpty(_request.ClientId))
                {
                    output.Write(ClientIdLen, 0, 2);
                    output.Write(ClientId, 0, ClientId.Length);
                    ByteBufHelper.WriteString(_request.ClientId, lenBuffer, output);
                    size += 1;
                }

                if (!string.IsNullOrEmpty(_request.ConsumerGroup))
                {
                    output.Write(ConsumerGroupLen, 0, 2);
                    output.Write(ConsumerGroup, 0, ConsumerGroup.Length);
                    ByteBufHelper.WriteString(_request.ConsumerGroup, lenBuffer, output);
                    size += 1;
                }

                output.Write(RequestTypeLen, 0, 2);
                output.Write(RequestType, 0, RequestType.Length);
                ByteBufHelper.WriteString(_request.RequestType.ToString(), lenBuffer, output);

                long end = output.Position;

                output.Position = start;
                ByteBufHelper.WriteInt16(size, lenBuffer, output);

                output.Position = end;

            }
        }
    }
}
