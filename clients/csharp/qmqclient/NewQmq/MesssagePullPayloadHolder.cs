using Qunar.TC.Qmq.Client.Codec;
using Qunar.TC.Qmq.Client.Codec.NewQmq;
using Qunar.TC.Qmq.Client.NewQmq.Model;
using System.IO;

namespace Qunar.TC.Qmq.Client.NewQmq
{
    internal class MesssagePullPayloadHolder : PayloadHolder
    {
        private readonly PullMessageRequest _request;

        public MesssagePullPayloadHolder(PullMessageRequest request)
        {
            _request = request;
        }

        public void Write(Stream output)
        {
            byte[] buffer = new byte[8];
            ByteBufHelper.WriteString(_request.Subject, buffer, output);
            ByteBufHelper.WriteString(_request.Group, buffer, output);
            ByteBufHelper.WriteString(_request.ConsumerId, buffer, output);
            ByteBufHelper.WriteInt32(_request.BatchSize, buffer, output);
            ByteBufHelper.WriteInt64(_request.Offset, buffer, output);
            ByteBufHelper.WriteInt64(_request.PullOffsetBegin, buffer, output);
            ByteBufHelper.WriteInt64(_request.PullOffsetEnd, buffer, output);
            ByteBufHelper.WriteInt64(_request.TimeoutMillis, buffer, output);
            ByteBufHelper.WriteByte(_request.IsBroadcast, output);
            WriteTags(_request, buffer, output);
        }

        private void WriteTags(PullMessageRequest request, byte[] buffer, Stream output)
        {
            ByteBufHelper.WriteInt16(_request.TagType, buffer, output);
            var tags = request.Tags;
            if (tags == null || tags.Length == 0)
            {
                ByteBufHelper.WriteByte((byte)0, output);
                return;
            }

            ByteBufHelper.WriteByte((byte)tags.Length, output);
            foreach (var tag in _request.Tags)
            {
                ByteBufHelper.WriteString(tag, buffer, output);
            }
        }
    }
}
