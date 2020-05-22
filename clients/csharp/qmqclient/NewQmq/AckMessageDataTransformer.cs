using Qunar.TC.Qmq.Client.Codec;
using Qunar.TC.Qmq.Client.Codec.NewQmq;
using Qunar.TC.Qmq.Client.Metainfo;
using Qunar.TC.Qmq.Client.NewQmq.Model;

namespace Qunar.TC.Qmq.Client.NewQmq
{
    internal class AckMessageDataTransformer : IDataTransformer
    {
        private static readonly NLog.Logger Logger = NLog.LogManager.GetCurrentClassLogger();

        public Datagram TransformRequest(Request request)
        {
            var header = new RemotingHeader
            {
                MagicCode = RemotingHeader.QmqMagicCode,
                Version = RemotingHeader.Version8,
                Code = CommandCode.AckRequest,
                Flag = 0,
                Opaque = (int)request.Id,
            };

            var payloadHolder = new MesssageAckPayloadHolder((AckRequest)request.Args);

            return new Datagram()
            {
                Header = header,
                PayloadHolder = payloadHolder,
            };
        }

        public Response TransformResponse(BrokerGroup brokerGroup, Datagram datagram)
        {
            switch (datagram.Header.Code)
            {
                case CommandCode.Success:
                    return new Response(datagram.Header.Opaque);
                default:
                    Logger.Error($"ack failed. broker: {brokerGroup.Name}, code: {datagram.Header.Code}");
                    return new Response(datagram.Header.Opaque, Response.Error);
            }
        }
    }
}