using System.Collections.Generic;
using Qunar.TC.Qmq.Client.Codec;
using Qunar.TC.Qmq.Client.Codec.NewQmq;
using Qunar.TC.Qmq.Client.Model;
using Qunar.TC.Qmq.Client.NewQmq.Model;
using Qunar.TC.Qmq.Client.Transport;
using Qunar.TC.Qmq.Client.Util;
using Qunar.TC.Qmq.Client.Metainfo;

namespace Qunar.TC.Qmq.Client.NewQmq
{
    internal class PullMessageDataTransformer : IDataTransformer
    {
        private static readonly NLog.Logger Logger = NLog.LogManager.GetCurrentClassLogger();

        public Datagram TransformRequest(Request request)
        {
            var header = new RemotingHeader
            {
                MagicCode = RemotingHeader.QmqMagicCode,
                Version = RemotingHeader.Version8,
                Code = CommandCode.PullMessage,
                Flag = 0,
                Opaque = (int)request.Id
            };
            var payloadHolder = new MesssagePullPayloadHolder((PullMessageRequest)request.Args);

            return new Datagram
            {
                Header = header,
                PayloadHolder = payloadHolder
            };
        }

        // TODO(keli.wang): return list of messages or a special object?
        public Response TransformResponse(BrokerGroup brokerGroup, Datagram datagram)
        {
            switch (datagram.Header.Code)
            {
                case CommandCode.Success:
                    return new Response(datagram.Header.Opaque)
                    {
                        Result = DeserializePulledMessages(brokerGroup, datagram.Body)
                    };
                case CommandCode.NoMessage:
                    return new Response(datagram.Header.Opaque)
                    {
                        Result = new List<BaseMessage>()
                    };
                default:
                    Logger.Error($"pull message failed. broker: {brokerGroup.Name}, code: {datagram.Header.Code}");
                    return new Response(datagram.Header.Opaque)
                    {
                        Result = new List<BaseMessage>()
                    };
            }
        }

        private static List<BaseMessage> DeserializePulledMessages(BrokerGroup brokerGroup, byte[] content)
        {
            var messages = new List<BaseMessage>();

            var packet = new Packet(content, 0, content.Length);
            if (packet.IsReadable())
            {
                var pullLogOffset = packet.ReadInt64();
                var consumerLogOffset = packet.ReadInt64();

                while (packet.IsReadable())
                {
                    var flag = (Flag)packet.ReadByte();
                    var createdTime = packet.ReadInt64();
                    var expiredTime = packet.ReadInt64();


                    var realSubject = RetrySubjectUtils.RealSubject(packet.ReadUTF8());
                    var messageId = packet.ReadUTF8();

                    var message = new BaseMessage(realSubject, messageId);
                    if (Flags.IsDelayMessage(flag))
                    {
                        message.SetDelayTime(DateTimeUtils.FromTime(expiredTime));
                    }
                    ReadTags(message, flag, packet);

                    var bodyLength = packet.ReadInt32();
                    var body = new byte[bodyLength];
                    packet.Read(body, 0, bodyLength);
                    var bodyPacket = new Packet(body, 0, bodyLength);
                    try
                    {
                        while (bodyPacket.IsReadable())
                        {
                            var key = bodyPacket.ReadUTF8();
                            var value = bodyPacket.ReadUTF8();
                            message.SetPropertyForInternal(key, value);
                        }
                    }
                    catch
                    {
                        message.SetProperty(BaseMessage.keys.qmq_corruptData, "true");
                    }

                    message.SetPropertyForInternal(BaseMessage.keys.qmq_createTime.ToString(), createdTime);
                    message.SetExpiredTime(DateTimeUtils.FromTime(expiredTime));
                    message.SetProperty(BaseMessage.keys.qmq_brokerGroupName, brokerGroup.Name);
                    message.SetProperty(BaseMessage.keys.qmq_pullOffset, pullLogOffset);
                    message.SetProperty(BaseMessage.keys.qmq_consumerOffset, consumerLogOffset);

                    messages.Add(message);
                    pullLogOffset++;
                    consumerLogOffset++;
                }
            }

            return messages;
        }

        static void ReadTags(BaseMessage message, Flag flag, Packet input)
        {
            if (!Flags.HasTag(flag)) return;

            int tagsSize = input.ReadByte();
            string[] tags = new string[tagsSize];
            for (int i = 0; i < tagsSize; i++)
            {
                string tag = input.ReadUTF8();
                tags[i] = tag;
            }

            message.SetTags(tags);
        }
    }
}