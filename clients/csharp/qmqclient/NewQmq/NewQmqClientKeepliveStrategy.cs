using Qunar.TC.Qmq.Client.Transport;
using Qunar.TC.Qmq.Client.Codec;
using Qunar.TC.Qmq.Client.Codec.NewQmq;

namespace Qunar.TC.Qmq.Client.NewQmq
{
    internal class NewQmqClientKeepliveStrategy : AbstractKeepliveStrategy
    {
        /// <summary>
        /// 间隔多久发心跳
        /// </summary>
        private const long PINGINTERVAL = 55 * 1000;
        private const long SESSIONTIMEOUT = 3 * 60 * 1000 + 2000;

        public NewQmqClientKeepliveStrategy() : base(PINGINTERVAL, SESSIONTIMEOUT)
        {
        }

        protected override void SessionTimeout(TransportClient client)
        {
            client.Close();
            client.CloseChannel();
        }

        protected override void Ping(TransportClient client)
        {
            var heartbeat = new Request(null);
            var header = new RemotingHeader()
            {
                MagicCode = RemotingHeader.QmqMagicCode,
                Version = RemotingHeader.Version8,
                Code = CommandCode.Heartbeat,
                Flag = 0,
                Opaque = (int)heartbeat.Id,
            };
            client.Send(new Datagram()
            {
                Header = header,
                PayloadHolder = null,
            });
        }
    }
}
