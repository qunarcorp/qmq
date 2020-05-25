package qunar.tc.qmq.consumer.pull;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.common.StatusSource;
import qunar.tc.qmq.common.SwitchWaiter;

import static qunar.tc.qmq.consumer.pull.DefaultPullEntry.DELAY_SCHEDULER;

/**
 * @author zhenwei.liu
 * @since 2019-10-15
 */
public class CompositePullEntry implements PullEntry, Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(CompositePullEntry.class);

    private final Map<String, PullEntry> pullEntryMap = Maps.newConcurrentMap();
    private final SwitchWaiter onlineSwitcher = new SwitchWaiter(false);
    private final AtomicBoolean isStarted = new AtomicBoolean(false);

    private final PushConsumerParam pushConsumerParam;
    private final PullService pullService;
    private final AckService ackService;
    private final BrokerService brokerService;
    private final PullStrategy pullStrategy;
    private final Executor executor;

    private volatile SettableFuture<Boolean> onlineFuture;

    public CompositePullEntry(PushConsumerParam pushConsumerParam, PullService pullService, AckService ackService,
                              BrokerService brokerService, PullStrategy pullStrategy, Executor executor) {
        this.pushConsumerParam = pushConsumerParam;
        this.pullService = pullService;
        this.ackService = ackService;
        this.brokerService = brokerService;
        this.pullStrategy = pullStrategy;
        this.executor = executor;
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
        synchronized (onlineSwitcher) {
            onlineSwitcher.on(src);
            final SettableFuture<Boolean> future = this.onlineFuture;
            if (future == null) return;
            future.set(true);
        }
        LOGGER.info("pullconsumer online. subject={}, group={}", getSubject(), getConsumerGroup());
        pullEntryMap.values().forEach(pe -> pe.online(src));
    }

    @Override
    public void offline(StatusSource src) {
        synchronized (onlineSwitcher) {
            onlineSwitcher.off(src);
        }

        LOGGER.info("pullconsumer offline. subject={}, group={}", getSubject(), getConsumerGroup());
        pullEntryMap.values().forEach(pe -> pe.offline(src));
    }

    @Override
    public void startPull() {
        if (isStarted.compareAndSet(false, true)) {
            executor.execute(this);
        }
    }

    @Override
    public void destroy() {
        isStarted.set(false);
        pullEntryMap.values().forEach(PullEntry::destroy);
    }

    private boolean await(ListenableFuture future) {
        if (future == null) {
            return false;
        }
        future.addListener(this, executor);
        return true;
    }

    private ListenableFuture waitOnline() {
        synchronized (onlineSwitcher) {
            if (onlineSwitcher.isOnline()) {
                return null;
            }

            final SettableFuture<Boolean> future = SettableFuture.create();
            this.onlineFuture = future;
            return future;
        }
    }

    private static class RunnableSettableFuture extends AbstractFuture implements Runnable {

        @Override
        public void run() {
            super.set(null);
        }
    }

    private ListenableFuture delay(long timeMillis) {
        RunnableSettableFuture future = new RunnableSettableFuture();
        DELAY_SCHEDULER.schedule(future, timeMillis, TimeUnit.MILLISECONDS);
        return future;
    }

    @Override
    public void run() {
        if (!isStarted.get()) return;
        if (await(waitOnline())) return;

        Thread thread = Thread.currentThread();
        String oldThreadName = thread.getName();
        thread.setName("qmq-pull-entry-" + getSubject() + "-" + getConsumerGroup());
        try {
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
                            pullEntry.startPull();
                            return pullEntry;
                        });
            }
        } finally {
            thread.setName(oldThreadName);
            await(delay(1000));
        }
    }
}
