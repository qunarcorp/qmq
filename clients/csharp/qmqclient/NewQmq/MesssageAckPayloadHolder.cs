using Qunar.TC.Qmq.Client.Codec;
using Qunar.TC.Qmq.Client.Codec.NewQmq;
using Qunar.TC.Qmq.Client.NewQmq.Model;
using System.IO;

namespace Qunar.TC.Qmq.Client.NewQmq
{
    class MesssageAckPayloadHolder : PayloadHolder
    {
        private AckRequest request;

        public MesssageAckPayloadHolder(AckRequest request)
        {
            this.request = request;
        }

        public void Write(Stream output)
        {
            byte[] buffer = new byte[8];
            ByteBufHelper.WriteString(request.Subject, buffer, output);
            ByteBufHelper.WriteString(request.Group, buffer, output);
            ByteBufHelper.WriteString(request.ConsumerId, buffer, output);
            ByteBufHelper.WriteInt64(request.PullOffsetBegin, buffer, output);
            ByteBufHelper.WriteInt64(request.PullOffsetEnd, buffer, output);
            ByteBufHelper.WriteByte((byte)(request.IsBroadcast ? 1 : 0), output);
        }
    }
}
