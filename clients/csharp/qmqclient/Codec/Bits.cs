// yuzhaohui
// 2016/7/20

namespace Qunar.TC.Qmq.Client.Codec
{
    internal static class Bits
	{
		public static short ReadInt16(byte[] buffer, int offset)
		{
			return (short)((short)(buffer[offset] << 8) +
			               (short)buffer[offset + 1]);
		}

		public static int ReadInt32(byte[] buffer, int offset)
		{
			return (buffer[offset] << 24) + 
			       (buffer[offset + 1] << 16) +
			       (buffer[offset + 2] << 8) +
			       buffer[offset + 3];
		}

		public static long ReadInt64(byte[] buffer, int offset)
		{
			return ((long)buffer[offset] << 56)
			       + ((long)buffer[offset + 1] << 48)
			       + ((long)buffer[offset + 2] << 40)
			       + ((long)buffer[offset + 3] << 32)
			       + ((long)buffer[offset + 4] << 24)
			       + ((long)buffer[offset + 5] << 16)
			       + ((long)buffer[offset + 6] << 8)
			       + (long)buffer[offset + 7];
		}

		public static void WriteInt16(short value, byte[] buffer, int offset)
		{
			buffer[offset] = (byte)(value >> 8);
			buffer[offset + 1] = (byte)(value & 0xff);
		}

		public static void WriteInt32(int value, byte[] buffer, int offset)
		{
			buffer[offset] = (byte)(value >> 24);
			buffer[offset + 1] = (byte)((value >> 16) & 0xff);
			buffer[offset + 2] = (byte)((value >> 8) & 0xff);
			buffer[offset + 3] = (byte)(value & 0xff);
		}

		public static void WriteInt64(long value, byte[] buffer, int offset)
		{
			buffer[offset] = (byte)(value >> 56);
			buffer[offset + 1] = (byte)((value >> 48) & 0xff);
			buffer[offset + 2] = (byte)((value >> 40) & 0xff);
			buffer[offset + 3] = (byte)((value >> 32) & 0xff);
			buffer[offset + 4] = (byte)((value >> 24) & 0xff);
			buffer[offset + 5] = (byte)((value >> 16) & 0xff);
			buffer[offset + 6] = (byte)((value >> 8) & 0xff);
			buffer[offset + 7] = (byte)(value & 0xff);
		}
    }
}
