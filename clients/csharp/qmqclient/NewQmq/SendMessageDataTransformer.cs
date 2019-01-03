using Qunar.TC.Qmq.Client.Codec.NewQmq;
using Qunar.TC.Qmq.Client.Exceptions;
using Qunar.TC.Qmq.Client.Transport;
using System.Collections;
using System.Collections.Generic;
using Qunar.TC.Qmq.Client.Codec;
using Qunar.TC.Qmq.Client.Metainfo;

namespace Qunar.TC.Qmq.Client.NewQmq
{
    internal class SendMessageDataTransformer : IDataTransformer
    {
        public Datagram TransformRequest(Request request)
        {
            var header = new RemotingHeader
            {
                MagicCode = RemotingHeader.QmqMagicCode,
                Version = RemotingHeader.Version8,
                Code = CommandCode.SendMessage,
                Flag = 0,
                Opaque = (int)request.Id // TODO(keli.wang): don't use request id here?
            };

            var payloadHolder = new MessageSendPayloadHolder((List<Message>)request.Args);

            return new Datagram
            {
                Header = header,
                PayloadHolder = payloadHolder
            };
        }

        public Response TransformResponse(BrokerGroup _, Datagram datagram)
        {
            switch (datagram.Header.Code)
            {
                case CommandCode.Success:
                    return new Response(datagram.Header.Opaque)
                    {
                        Result = DeserializeSendResult(datagram.Body)
                    };
                case CommandCode.BrokerReject:
                    return new Response(datagram.Header.Opaque, Response.Error)
                    {
                        Result = new BrokerRejectException()
                    };
                default:
                    return new Response(datagram.Header.Opaque, Response.Error)
                    {
                        Result = new RemoteException()
                    };
            }
        }

        private Hashtable DeserializeSendResult(byte[] data)
        {
            var table = new Hashtable();

            var packet = new Packet(data, 0, data.Length);
            while (packet.IsReadable())
            {
                var messageId = packet.ReadUTF8();
                var code = packet.ReadInt32();
                var remark = packet.ReadUTF8();

                switch (code)
                {
                    case SendMessageResponseCode.SUCCESS:
                        break;
                    case SendMessageResponseCode.DUPLICATE:
                        table.Add(messageId, new DuplicateMessageException(messageId));
                        break;
                    case SendMessageResponseCode.BROKER_REJECT:
                        table.Add(messageId, new BrokerRejectException());
                        break;
                    case SendMessageResponseCode.BROKER_READ_ONLY:
                        table.Add(messageId, new BrokerReadOnlyException());
                        break;
                    case SendMessageResponseCode.BUSY:
                    case SendMessageResponseCode.SUBJECT_NOT_ASSIGNED:
                    case SendMessageResponseCode.BROKER_SAVE_FAILED:
                        table.Add(messageId, new MessageException(messageId, remark));
                        break;
                    default:
                        break;
                };
            }

            return table;
        }
    }
}
