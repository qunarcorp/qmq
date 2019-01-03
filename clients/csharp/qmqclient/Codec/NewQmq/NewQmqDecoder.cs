using Qunar.TC.Qmq.Client.Transport;
using System.IO;

namespace Qunar.TC.Qmq.Client.Codec.NewQmq
{
    internal class NewQmqDecoder : Decoder
    {
        /// <summary>
        ///  |total size|header size|magic code| header | body |
        /// </summary>
        /// <param name="stream"></param>
        /// <returns></returns>
        public object Decode(InputStream stream)
        {
            if (!stream.CanRead)
                return Response.NeedMore;

            long remain = stream.Length;
            if (remain < RemotingHeader.MinHeaderSize + RemotingHeader.LengthFieldLen)
            {
                return Response.NeedMore;
            }

            stream.MarkReaderIndex();

            //go to magic code field
            stream.Position = stream.Position + RemotingHeader.LengthFieldLen;

            var buffer = new byte[4];
            var magicCode = ByteBufHelper.ReadInt32(buffer, stream);

            //go to head, length field
            stream.ResetReaderIndex();
            if (RemotingHeader.QmqMagicCode != magicCode)
            {
                throw new IOException("非法数据");
            }

            stream.MarkReaderIndex();
            var total = ByteBufHelper.ReadInt32(buffer, stream);
            //fuck this
            if (remain - RemotingHeader.TotalSizeLen < total)
            {
                stream.ResetReaderIndex();
                return Response.NeedMore;
            }

            short headerSize = ByteBufHelper.ReadInt16(buffer, stream);
            var header = DecodeHeader(buffer, stream);

            int bodyLen = total - headerSize - RemotingHeader.HeaderSizeLen;
            byte[] body = new byte[bodyLen];
            stream.Read(body, 0, bodyLen);

            var msg = new Datagram
            {
                Body = body,
                Header = header
            };

            return msg;
        }

        private RemotingHeader DecodeHeader(byte[] buffer, InputStream stream)
        {
            var header = new RemotingHeader();

            //skip magic code
            ByteBufHelper.ReadInt32(buffer, stream);

            header.Code = ByteBufHelper.ReadInt16(buffer, stream);
            header.Version = ByteBufHelper.ReadInt16(buffer, stream);
            header.Opaque = ByteBufHelper.ReadInt32(buffer, stream);
            header.Flag = ByteBufHelper.ReadInt32(buffer, stream);
            header.RequestCode = ByteBufHelper.ReadInt16(buffer, stream);
            return header;
        }
    }
}
