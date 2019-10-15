package qunar.tc.qmq.consumer.pull;

import com.google.common.base.Joiner;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.CompositePullClient;
import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.PullClient;

/**
 * @author zhenwei.liu
 * @since 2019-09-30
 */
public abstract class AbstractCompositePullClient<T extends PullClient> extends AbstractPullClient implements
        CompositePullClient<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractCompositePullClient.class);

    private List<T> components;

    public AbstractCompositePullClient(String subject, String consumerGroup, String brokerGroup, String consumerId,
            ConsumeStrategy consumeStrategy, int allocationVersion, boolean isBroadcast, boolean isOrdered,
            int partitionSetVersion, long consumptionExpiredTime, List<T> components) {
        super(subject, consumerGroup, createCompositePartitions(components), brokerGroup, consumerId, consumeStrategy,
                allocationVersion, isBroadcast, isOrdered, partitionSetVersion, consumptionExpiredTime);
        this.components = components;
    }

    private static String createCompositePartitions(List<? extends PullClient> components) {
        return Joiner.on("|").join(components.stream().map(PullClient::getPartitionName).collect(Collectors.toList()));
    }


    @Override
    public List<T> getComponents() {
        return components;
    }

    @Override
    public void setComponents(List<T> components) {
        this.components = components;
    }

    @Override
    public void startPull(ExecutorService executor) {
        for (T component : components) {
            doIgnoreException(() -> component.startPull(executor));
        }
    }

    @Override
    public void destroy() {
        for (T component : components) {
            doIgnoreException(component::destroy);
        }
    }

    @Override
    public void online() {
        for (T component : components) {
            doIgnoreException(component::online);
        }
    }

    @Override
    public void offline() {
        for (T component : components) {
            doIgnoreException(component::offline);
        }
    }

    @Override
    public void setConsumerAllocationVersion(int version) {
        for (T component : components) {
            component.setConsumerAllocationVersion(version);
        }
    }

    @Override
    public void setConsumptionExpiredTime(long timestamp) {
        for (T component : components) {
            component.setConsumptionExpiredTime(timestamp);
        }
    }

    @Override
    public void setConsumeStrategy(ConsumeStrategy consumeStrategy) {
        for (T component : components) {
            component.setConsumeStrategy(consumeStrategy);
        }
    }

    private void doIgnoreException(Runnable runnable) {
        try {
            runnable.run();
        } catch (Throwable t) {
            LOGGER.error("error ", t);
        }
    }
}
