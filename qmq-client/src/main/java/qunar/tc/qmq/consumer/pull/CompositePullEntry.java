package qunar.tc.qmq.consumer.pull;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.common.StatusSource;
import qunar.tc.qmq.common.SwitchWaiter;

/**
 * @author zhenwei.liu
 * @since 2019-10-15
 */
public class CompositePullEntry implements PullEntry {

    private static final Logger LOGGER = LoggerFactory.getLogger(CompositePullEntry.class);

    private final Map<String, PullEntry> pullEntryMap = Maps.newConcurrentMap();
    private final SwitchWaiter onlineSwitcher = new SwitchWaiter(false);
    private final AtomicBoolean isStarted = new AtomicBoolean(false);

    private final PushConsumerParam pushConsumerParam;
    private final PullService pullService;
    private final AckService ackService;
    private final BrokerService brokerService;
    private final PullStrategy pullStrategy;

    public CompositePullEntry(PushConsumerParam pushConsumerParam, PullService pullService, AckService ackService,
                              BrokerService brokerService, PullStrategy pullStrategy) {
        this.pushConsumerParam = pushConsumerParam;
        this.pullService = pullService;
        this.ackService = ackService;
        this.brokerService = brokerService;
        this.pullStrategy = pullStrategy;
    }

    @Override
    public String getSubject() {
        return pushConsumerParam.getSubject();
    }

    @Override
    public String getConsumerGroup() {
        return pushConsumerParam.getGroup();
    }

    @Override
    public void online(StatusSource src) {
        onlineSwitcher.on(src);
        LOGGER.info("pullconsumer online. subject={}, group={}", getSubject(), getConsumerGroup());
        pullEntryMap.values().forEach(pe -> pe.online(src));
    }

    @Override
    public void offline(StatusSource src) {
        onlineSwitcher.off(src);
        LOGGER.info("pullconsumer offline. subject={}, group={}", getSubject(), getConsumerGroup());
        pullEntryMap.values().forEach(pe -> pe.offline(src));
    }

    @Override
    public void startPull(Executor executor) {
        executor.execute(() -> {
            if (isStarted.compareAndSet(false, true)) {
                while (isStarted.get()) {
                    if (onlineSwitcher.waitOn()) {
                        BrokerClusterInfo brokerCluster = brokerService
                                .getClusterBySubject(ClientType.CONSUMER, getSubject(), getConsumerGroup());
                        for (BrokerGroupInfo brokerGroup : brokerCluster.getGroups()) {
                            String brokerGroupName = brokerGroup.getGroupName();
                            pullEntryMap.computeIfAbsent(brokerGroupName,
                                    bgn -> {
                                        PushConsumer pushConsumer = new PushConsumerImpl(pushConsumerParam.getSubject(), pushConsumerParam.getGroup(), pushConsumerParam.getRegistParam());
                                        DefaultPullEntry pullEntry = new DefaultPullEntry(brokerGroupName,
                                                pushConsumer, pullService, ackService,
                                                brokerService, pullStrategy, onlineSwitcher, executor);
                                        pullEntry.startPull(executor);
                                        return pullEntry;
                                    });
                        }
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        // ignore
                        LOGGER.warn("sleep interrupted", e);
                    }
                }
            }
        });
    }

    @Override
    public void destroy() {
        isStarted.set(false);
        pullEntryMap.values().forEach(PullEntry::destroy);
    }
}
