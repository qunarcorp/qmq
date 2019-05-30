package qunar.tc.qmq.backup.base;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019/5/29
 */
public enum RecordEnum {
    RECORD((byte) 0),
    RETRY_RECORD((byte) 1),
    DEAD_RECORD((byte) 2),
    OTHER((byte) -1);

    private byte code;

    RecordEnum(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }
}
