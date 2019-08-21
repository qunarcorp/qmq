package qunar.tc.qmq;

/**
 * @author zhenwei.liu
 * @since 2019-08-21
 */
public interface Retriable {

    int getTries();

    int getMaxTries();
}
