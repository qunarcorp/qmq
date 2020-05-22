using System.IO;
using System.Text;
using Qunar.TC.Qmq.Client.Util;

namespace Qunar.TC.Qmq.Client.Codec
{
    static class ByteBufHelper
    {
        public static short ReadInt16(byte[] buffer, Stream stream)
        {
            stream.Read(buffer, 0, 2);
            return Bits.ReadInt16(buffer, 0);
        }

        public static int ReadInt32(byte[] buffer, Stream stream)
        {
            stream.Read(buffer, 0, 4);
            return Bits.ReadInt32(buffer, 0);
        }

        public static void WriteInt64(long value, byte[] target, Stream output)
        {
            Bits.WriteInt64(value, target, 0);
            output.Write(target, 0, 8);
        }

        public static void WriteInt32(int value, byte[] target, Stream output)
        {
            Bits.WriteInt32(value, target, 0);
            output.Write(target, 0, 4);
        }

        public static void WriteInt16(short value, byte[] target, Stream output)
        {
            Bits.WriteInt16(value, target, 0);
            output.Write(target, 0, 2);
        }

        public static void WriteByte(byte value, Stream output)
        {
            output.WriteByte(value);
        }

        public static void WriteByte(bool value, Stream output)
        {
            byte bit = value ? (byte)1 : (byte)0;
            output.WriteByte(bit);
        }

        public static void WriteString(string value, byte[] lenBuffer, Stream output)
        {
            var buffer = Encoding.UTF8.GetBytes(value);
            WriteInt16((short)buffer.Length, lenBuffer, output);
            output.Write(buffer, 0, buffer.Length);
        }

        public static void WriteString(object value, byte[] lenBuffer, Stream output)
        {
            string input;
            switch (value)
            {
                case bool _:
                    input = value.ToString().ToLower();
                    break;
                case float f:
                    if (float.IsPositiveInfinity(f))
                    {
                        input = FloatingParser.JavaPositiveInfinity;
                    }
                    else if (float.IsNegativeInfinity(f))
                    {
                        input = FloatingParser.JavaNegativeInfinity;
                    }
                    else
                    {
                        input = f.ToString("G9");
                    }
                    break;
                case double d:
                    if (double.IsPositiveInfinity(d))
                    {
                        input = FloatingParser.JavaPositiveInfinity;
                    }
                    else if (double.IsNegativeInfinity(d))
                    {
                        input = FloatingParser.JavaNegativeInfinity;
                    }
                    else
                    {
                        input = d.ToString("G17");
                    }
                    break;
                default:
                    input = value.ToString();
                    break;
            }

            WriteString(input, lenBuffer, output);
        }
    }
}
