// yuzhaohui
// 12/5/2016
using Qunar.TC.Qmq.Client.Codec;
using System;
using System.Text;

namespace Qunar.TC.Qmq.Client.Transport
{
    class Packet
    {
        public readonly byte[] buffer;

        public int readerIndex;

        public int writerIndex;

        private Pool _parent;

        public Packet(byte[] buffer, int readerIndex, int writerIndex)
        {
            this.buffer = buffer;
            this.readerIndex = readerIndex;
            this.writerIndex = writerIndex;
        }

        public void Write(Packet input)
        {
            var count = input.Len;
            if (count <= 8)
            {
                for (var i = 0; i < count; ++i)
                {
                    buffer[writerIndex + i] = input.buffer[input.readerIndex + i];
                }
                writerIndex += count;
                return;
            }

            Buffer.BlockCopy(input.buffer, input.readerIndex, buffer, writerIndex, count);
            writerIndex += count;
        }

        public int Read(byte[] buffer, int offset, int count)
        {
            count = Math.Min(Len, count);

            if (count <= 8)
            {
                for (var i = 0; i < count; ++i)
                {
                    buffer[offset + i] = this.buffer[this.readerIndex + i];
                }
                readerIndex += count;
                return count;
            }

            Buffer.BlockCopy(this.buffer, readerIndex, buffer, offset, count);
            this.readerIndex += count;
            return count;
        }

        public int ReadByte()
        {
            if (readerIndex >= writerIndex)
            {
                return -1;
            }
            return buffer[readerIndex++];
        }

        public short ReadInt16()
        {
            var result = Bits.ReadInt16(buffer, readerIndex);
            readerIndex += 2;
            return result;
        }

        public int ReadInt32()
        {
            var result = Bits.ReadInt32(buffer, readerIndex);
            readerIndex += 4;
            return result;
        }

        public long ReadInt64()
        {
            var result = Bits.ReadInt64(buffer, readerIndex);
            readerIndex += 8;
            return result;
        }

        public void DiscardRead()
        {
            if (Len > readerIndex) return;
            var len = Len;
            for (var i = 0; i < len; ++i)
            {
                buffer[i] = buffer[readerIndex + i];
            }
            readerIndex = 0;
            writerIndex = len;
        }

        public string ReadUTF8()
        {
            var len = ReadInt16();
            var result = Encoding.UTF8.GetString(buffer, readerIndex, len);
            readerIndex += len;
            return result;
        }

        public bool IsReadable()
        {
            return Len > 0;
        }

        public int Len
        {
            get
            {
                return writerIndex - readerIndex;
            }
        }

        public int Remain
        {
            get
            {
                return buffer.Length - writerIndex;
            }
        }

        public Pool Pool
        {
            set
            {
                _parent = value;
            }
        }

        public void Release()
        {
            if (_parent == null) return;
            _parent.Return(this);
        }
    }
}
