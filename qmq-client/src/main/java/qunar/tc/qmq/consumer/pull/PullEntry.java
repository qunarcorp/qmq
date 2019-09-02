package qunar.tc.qmq.consumer.pull;

import qunar.tc.qmq.common.StatusSource;

/**
 * @author zhenwei.liu
 * @since 2019-09-02
 */
public interface PullEntry extends PullClient {

    void online(StatusSource statusSource);

    void offline(StatusSource statusSource);
}
