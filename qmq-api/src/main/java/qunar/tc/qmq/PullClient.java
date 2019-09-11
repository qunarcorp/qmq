package qunar.tc.qmq;

import java.util.concurrent.ExecutorService;

/**
 * @author zhenwei.liu
 * @since 2019-09-02
 */
public interface PullClient {

    String getSubject();

    String getConsumerGroup();

    String getPartitionName();

    int getVersion();

    void setVersion(int version);

    void startPull(ExecutorService executor);

    void destroy();

    void online(StatusSource statusSource);

    void offline(StatusSource statusSource);
}
