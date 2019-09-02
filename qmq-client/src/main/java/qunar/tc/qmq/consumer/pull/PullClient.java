package qunar.tc.qmq.consumer.pull;

import java.util.concurrent.ExecutorService;

/**
 * @author zhenwei.liu
 * @since 2019-09-02
 */
public interface PullClient {

    void startPull(ExecutorService executor);

    void destroy();
}
