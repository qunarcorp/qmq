package qunar.tc.qmq.backup.base;

/**
 * pull or ack
 *
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019-02-27 20:10
 */
public enum ActionEnum {
    PULL((byte) 0),
    ACK((byte) 1),
    OTHER((byte) -1),
    OMIT((byte) -2);

    private byte code;

    ActionEnum(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }

}
