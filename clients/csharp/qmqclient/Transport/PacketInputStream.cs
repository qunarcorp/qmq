// yuzhaohui
// 12/12/2016
using System;
using System.IO;
namespace Qunar.TC.Qmq.Client.Transport
{
    class PacketInputStream : InputStream
    {
        private Packet packet;

        private int markWriterIndex;

        private int markReaderIndex;

        public PacketInputStream(Packet packet)
        {
            this.packet = packet;
        }

        public override bool CanRead
        {
            get
            {
                return packet.IsReadable();
            }
        }

        public override bool CanSeek
        {
            get
            {
                return false;
            }
        }

        public override bool CanWrite
        {
            get
            {
                return true;
            }
        }

        public override long Length
        {
            get
            {
                return packet.Len;
            }
        }

        public override long Position
        {
            get
            {
                return packet.readerIndex;
            }

            set
            {
                packet.readerIndex = (int)value;
            }
        }

        public override void Flush()
        {

        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            return packet.Read(buffer, offset, count);
        }

        public override int ReadByte()
        {
            return packet.ReadByte();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }

        internal override void MarkWriterIndex()
        {
            markWriterIndex = packet.writerIndex;
        }

        internal override void ResetWriterIndex()
        {
            packet.writerIndex = markWriterIndex;
        }

        internal override void MarkReaderIndex()
        {
            markReaderIndex = packet.readerIndex;
        }

        internal override void ResetReaderIndex()
        {
            packet.readerIndex = markReaderIndex;
        }

        internal override void Limit(int len)
        {
            packet.writerIndex = packet.readerIndex + len;
        }
    }
}
