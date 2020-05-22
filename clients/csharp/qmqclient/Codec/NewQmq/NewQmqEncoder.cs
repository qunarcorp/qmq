using System.IO;

namespace Qunar.TC.Qmq.Client.Codec.NewQmq
{
    internal class NewQmqEncoder : Encoder
    {
        public void Encode(object obj, Stream stream)
        {
            var msg = obj as Datagram;
            if (msg == null) return;

            int start = (int)stream.Position;
            int headerStart = start + RemotingHeader.LengthFieldLen;
            stream.Position = headerStart;

            byte[] buffer = new byte[4];
            EncodeHeader(msg.Header, buffer, stream);
            int headerSize = (int)stream.Position - headerStart;

            msg.WriteBody(stream);
            int end = (int)stream.Position;
            int total = end - start - RemotingHeader.TotalSizeLen;

            stream.Position = start;


            ByteBufHelper.WriteInt32(total, buffer, stream);

            ByteBufHelper.WriteInt16((short)headerSize, buffer, stream);

            stream.Position = end;
        }

        private void EncodeHeader(RemotingHeader header, byte[] buffer, Stream stream)
        {
            ByteBufHelper.WriteInt32(RemotingHeader.QmqMagicCode, buffer, stream);
            ByteBufHelper.WriteInt16(header.Code, buffer, stream);
            ByteBufHelper.WriteInt16(header.Version, buffer, stream);
            ByteBufHelper.WriteInt32(header.Opaque, buffer, stream);
            ByteBufHelper.WriteInt32(header.Flag, buffer, stream);
            ByteBufHelper.WriteInt16(header.RequestCode, buffer, stream);
        }
    }
}
