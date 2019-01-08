package qunar.tc.qmq.delay.sender.loadbalance;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019-01-08 16:03
 */
public class InSendingNumWeightLoadBalancer extends RandomLoadBalancer {
    private static final Logger LOG = LoggerFactory.getLogger(InSendingNumWeightLoadBalancer.class);

    private final LoadBalanceStats stats;

    private volatile List<Long> accumulatedWeights = Collections.synchronizedList(new ArrayList<>());

    private volatile List<BrokerGroupInfo> brokerGroups = Collections.synchronizedList(new ArrayList<>());

    private final AtomicBoolean brokerWeightAssignmentInProgress = new AtomicBoolean(false);

    private final Timer brokerWeightTimer;

    private final Random random = new Random();

    public InSendingNumWeightLoadBalancer() {
        stats = new LoadBalanceStats();
        brokerWeightTimer = new Timer("brokerWeightTimer", true);
        scheduleBrokerWeight();
    }

    private void scheduleBrokerWeight() {
        brokerWeightTimer.schedule(new DynamicBrokerGroupWeightTask(), 0, 1000);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Stopping brokerWeightTimer.");
            brokerWeightTimer.cancel();
        }));
    }

    @Override
    public BrokerGroupInfo select(BrokerClusterInfo clusterInfo) {
        List<BrokerGroupInfo> arrivalGroups = clusterInfo.getGroups();
        if (arrivalGroups == null || arrivalGroups.isEmpty()) return null;

        List<BrokerGroupInfo> stayGroupInfos = getBrokerGroups();
        int groupsSize = arrivalGroups.size();
        refreshBrokerGroups(arrivalGroups, stayGroupInfos);

        BrokerGroupInfo brokerGroupInfo = null;
        List<Long> currentWeights = getAccumulatedWeights();
        int cyclicCount = 0;
        while (brokerGroupInfo == null && cyclicCount++ < groupsSize * 3) {
            int brokerIndex = 0;
            long maxTotalWeight = currentWeights.size() == 0 ? 0 : currentWeights.get(currentWeights.size() - 1);
            if (maxTotalWeight < 1000) {
                brokerGroupInfo = super.select(clusterInfo);
            } else {
                long randomWeight = random.nextLong() * maxTotalWeight;
                int n = 0;
                for (Long l : currentWeights) {
                    if (l >= randomWeight) {
                        brokerIndex = n;
                        break;
                    } else {
                        ++n;
                    }
                }

                brokerGroupInfo = stayGroupInfos.get(brokerIndex);

                if (brokerGroupInfo == null) {
                    Thread.yield();
                    continue;
                }

                if (brokerGroupInfo.isAvailable()) {
                    return brokerGroupInfo;
                }

                brokerGroupInfo = null;
            }
        }

        return brokerGroupInfo;
    }

    private void refreshBrokerGroups(List<BrokerGroupInfo> arrivalGroups, List<BrokerGroupInfo> stayBrokerGroups) {
        Set<BrokerGroupInfo> oldSet = Sets.newHashSet(stayBrokerGroups);
        Set<BrokerGroupInfo> newSet = Sets.newHashSet(arrivalGroups);
        Set<BrokerGroupInfo> removals = Sets.difference(oldSet, newSet);
        Set<BrokerGroupInfo> adds = Sets.difference(newSet, oldSet);
        if (!removals.isEmpty() || !adds.isEmpty()) {
            List<BrokerGroupInfo> attached = ImmutableList.copyOf(stayBrokerGroups);
            attached.removeAll(removals);
            attached.addAll(adds);
            setBrokerGroups(attached);
        }
    }

    class DynamicBrokerGroupWeightTask extends TimerTask {

        @Override
        public void run() {
            BrokerWeight brokerWeight = new BrokerWeight();
            try {
                brokerWeight.maintainWeights();
            } catch (Exception e) {
                LOG.error("Error running DynamicBrokerGroupWeightTask.", e);
            }
        }
    }

    class BrokerWeight {
        void maintainWeights() {
            if (!brokerWeightAssignmentInProgress.compareAndSet(false, true)) {
                return;
            }

            try {
                LOG.info("weight adjusting job started.");
                doMaintain();
            } catch (Exception e) {
                LOG.error("Error calculating broker weights.");
            } finally {
                brokerWeightAssignmentInProgress.set(false);
            }
        }

        private void doMaintain() {
            long total = 0;
            List<BrokerGroupInfo> groups = getBrokerGroups();
            for (BrokerGroupInfo brokerGroup : groups) {
                final BrokerGroupStats brokerGroupStats = stats.getBrokerGroupStats(brokerGroup);
                total += brokerGroupStats.getToSendCount();
            }

            long weightSoFar = 0;
            List<Long> finalWeights = Lists.newArrayListWithCapacity(groups.size());
            for (BrokerGroupInfo brokerGroup : groups) {
                final BrokerGroupStats brokerGroupStats = stats.getBrokerGroupStats(brokerGroup);
                long weight = total - brokerGroupStats.getToSendCount();
                weightSoFar += weight;
                finalWeights.add(weightSoFar);
            }
            setAccumulatedWeights(finalWeights);
        }
    }

    @Override
    public BrokerGroupStats getBrokerGroupStats(BrokerGroupInfo brokerGroupInfo) {
        return stats.getBrokerGroupStats(brokerGroupInfo);
    }

    private void setAccumulatedWeights(final List<Long> weights) {
        this.accumulatedWeights = weights;
    }

    private void setBrokerGroups(final List<BrokerGroupInfo> brokerGroups) {
        this.brokerGroups = brokerGroups;
    }

    private List<BrokerGroupInfo> getBrokerGroups() {
        return Collections.unmodifiableList(brokerGroups);
    }

    private List<Long> getAccumulatedWeights() {
        return Collections.unmodifiableList(accumulatedWeights);
    }

}
