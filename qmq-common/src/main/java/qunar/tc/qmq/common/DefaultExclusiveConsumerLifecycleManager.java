package qunar.tc.qmq.common;

import com.google.common.base.Joiner;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;

import java.util.concurrent.TimeUnit;

/**
 * @author zhenwei.liu
 * @since 2019-09-05
 */
public class DefaultExclusiveConsumerLifecycleManager implements ExclusiveConsumerLifecycleManager {

    private static final Joiner keyJoiner = Joiner.on(":");

    private final ExpiringMap<String, Integer> cache = ExpiringMap.create();

    @Override
    public boolean isAlive(String subject, String consumerGroup, String brokerGroup, String partitionName) {
        String key = createKey(subject, consumerGroup, brokerGroup, partitionName);
        Integer oldVersion = cache.get(key);
        return oldVersion != null;
    }

    @Override
    public boolean refreshLifecycle(String subject, String consumerGroup, String brokerGroup, String partitionName, int version, long expired) {
        String key = createKey(subject, consumerGroup, brokerGroup, partitionName);
        synchronized (key.intern()) {
            Integer oldVersion = cache.get(key);
            if (oldVersion == null || oldVersion <= version) {
                cache.put(key, version, ExpirationPolicy.CREATED, expired - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
                return true;
            }
        }
        return false;
    }

    private static String createKey(String subject, String consumerGroup, String brokerGroup, String partitionName) {
        return keyJoiner.join(subject, consumerGroup, brokerGroup, partitionName);
    }
}
