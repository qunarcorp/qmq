package qunar.tc.qmq.consumer.pull;

import qunar.tc.qmq.PullClient;
import qunar.tc.qmq.protocol.consumer.ConsumerMetaInfoResponse;

import java.util.Collection;

/**
 * @author zhenwei.liu
 * @since 2019-09-27
 */
public interface PullClientManager<T extends PullClient> {

    void updateClient(ConsumerMetaInfoResponse response, Object registryParam, boolean isAutoOnline);

    T getPullClient(String subject, String consumerGroup);

    Collection<T> getPullClients();
}
