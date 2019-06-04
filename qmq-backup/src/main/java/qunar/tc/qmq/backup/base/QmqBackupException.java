package qunar.tc.qmq.backup.base;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-11-27 20:17
 */
public class QmqBackupException extends RuntimeException {
    public QmqBackupException(String message) {
        super(message);
    }

    public QmqBackupException(Throwable cause) {
        super(cause);
    }
}
