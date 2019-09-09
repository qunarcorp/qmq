package qunar.tc.qmq;

import java.util.concurrent.ExecutorService;

/**
 * @author zhenwei.liu
 * @since 2019-09-02
 */
public interface PullClient {

    String getSubject();

    String getConsumerGroup();

    String getSubjectSuffix();

    String getBrokerGroup();

    int getVersion();

    ConsumeMode getConsumeMode();

    void startPull(ExecutorService executor);

    void destroy();

    void online(StatusSource statusSource);

    void offline(StatusSource statusSource);
}
