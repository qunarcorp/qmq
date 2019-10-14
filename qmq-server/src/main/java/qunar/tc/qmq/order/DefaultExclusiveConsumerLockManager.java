package qunar.tc.qmq.order;

import com.google.common.base.Joiner;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhenwei.liu
 * @since 2019-08-30
 */
public class DefaultExclusiveConsumerLockManager implements ExclusiveConsumerLockManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultExclusiveConsumerLockManager.class);

    private static final Joiner keyJoiner = Joiner.on(":");

    private final Cache<String, String> lockCache;

    public DefaultExclusiveConsumerLockManager(long lockExpireDuration, TimeUnit timeUnit) {
        this.lockCache = CacheBuilder.newBuilder()
                .expireAfterWrite(lockExpireDuration, timeUnit)
                .build();
    }

    @Override
    public boolean acquireLock(String partitionName, String consumerGroup, String clientId) {
        String lockKey = createLockKey(partitionName, consumerGroup);
        synchronized (lockKey.intern()) {
            try {
                String oldClient = lockCache.get(lockKey, () -> {
                    LOGGER.info("acquire new lock for partition {} consumerGroup {} clientId {}", partitionName, consumerGroup, clientId);
                    return clientId;
                });
                if (Objects.equals(clientId, oldClient)) {
                    // refresh MetaInfo expired duration
                    lockCache.put(lockKey, clientId);
                    return true;
                } else {
                    return false;
                }
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public boolean refreshLock(String partitionName, String consumerGroup, String clientId) {
        String lockKey = createLockKey(partitionName, consumerGroup);
        synchronized (lockKey.intern()) {
            String oldClient = lockCache.getIfPresent(lockKey);
            if (Objects.equals(clientId, oldClient)) {
                lockCache.put(lockKey, clientId);
                return true;
            } else {
                return false;
            }
        }
    }

    @Override
    public boolean releaseLock(String partitionName, String consumerGroup, String clientId) {
        String lockKey = createLockKey(partitionName, consumerGroup);
        synchronized (lockKey.intern()) {
            String oldClient = lockCache.getIfPresent(lockKey);
            if (Objects.equals(clientId, oldClient)) {
                lockCache.invalidate(lockKey);
                LOGGER.info("release lock for partition {} consumerGroup {} clientId {}", partitionName, consumerGroup, clientId);
                return true;
            } else {
                return false;
            }
        }
    }

    private String createLockKey(String partitionName, String consumerGroup) {
        return keyJoiner.join(partitionName, consumerGroup);
    }
}
