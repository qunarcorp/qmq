package qunar.tc.qmq.order;

import com.google.common.base.Joiner;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author zhenwei.liu
 * @since 2019-08-30
 */
public class DefaultExclusiveMessageLockManager implements ExclusiveMessageLockManager {

    private static final Joiner keyJoiner = Joiner.on(":");

    private final Cache<String, String> lockCache;

    public DefaultExclusiveMessageLockManager(long lockExpireDuration, TimeUnit timeUnit) {
        this.lockCache = CacheBuilder.newBuilder()
                .expireAfterWrite(lockExpireDuration, timeUnit)
                .build();
    }

    @Override
    public boolean acquireLock(String partitionName, String consumerGroup, String clientId) {
        String lockKey = createLockKey(partitionName, consumerGroup);
        synchronized (lockKey.intern()) {
            try {
                String oldClient = lockCache.get(lockKey, () -> clientId);
                if (Objects.equals(clientId, oldClient)) {
                    // refresh expired duration
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
    public boolean releaseLock(String partitionName, String consumerGroup, String clientId) {
        String lockKey = createLockKey(partitionName, consumerGroup);
        synchronized (lockKey.intern()) {
            String oldClient = lockCache.getIfPresent(lockKey);
            if (Objects.equals(clientId, oldClient)) {
                lockCache.invalidate(lockKey);
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
