package qunar.tc.qmq.utils;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019/5/31
 */
public class Bytes {
    public static byte[] long2bytes(long v) {
        byte[] ret = {0, 0, 0, 0, 0, 0, 0, 0};
        long2bytes(v, ret);
        return ret;
    }

    public static void long2bytes(long v, byte[] b) {
        setLong(b, v, 0);
    }

    public static void setLong(final byte[] b, final long n, final int offset) {
        b[offset + 0] = (byte) (n >>> 56);
        b[offset + 1] = (byte) (n >>> 48);
        b[offset + 2] = (byte) (n >>> 40);
        b[offset + 3] = (byte) (n >>> 32);
        b[offset + 4] = (byte) (n >>> 24);
        b[offset + 5] = (byte) (n >>> 16);
        b[offset + 6] = (byte) (n >>> 8);
        b[offset + 7] = (byte) (n >>> 0);
    }

    public static long bytes2long(final byte[] b) {
        return getLong(b, 0);
    }

    public static long getLong(final byte[] b, final int offset) {
        return (b[offset + 0] & 0xFFL) << 56 | (b[offset + 1] & 0xFFL) << 48 | (b[offset + 2] & 0xFFL) << 40 | (b[offset + 3] & 0xFFL) << 32 | (b[offset + 4] & 0xFFL) << 24
                | (b[offset + 5] & 0xFFL) << 16 | (b[offset + 6] & 0xFFL) << 8 | (b[offset + 7] & 0xFFL) << 0;
    }

}
