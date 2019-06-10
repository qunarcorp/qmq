package qunar.tc.qmq.utils;

/**
 * Created by zhaohui.yu
 * 8/22/18
 */
public class Flags {
    public static byte setDelay(byte flag, boolean isDelay) {
        return !isDelay ? flag : (byte) (flag | 2);
    }

    public static byte setReliability(byte flag, boolean isReliability) {
        return isReliability ? flag : (byte) (flag | 1);
    }

    public static byte setTags(byte flag, boolean hasTag) {
        return hasTag ? (byte) (flag | 4) : flag;
    }

    public static boolean isDelay(byte flag) {
        return (flag & 2) == 2;
    }

    public static boolean isReliability(byte flag) {
        return (flag & 1) != 1;
    }

    public static boolean hasTags(byte flag) {
        return (flag & 4) == 4;
    }
}
