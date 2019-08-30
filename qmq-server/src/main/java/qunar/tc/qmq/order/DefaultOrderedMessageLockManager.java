package qunar.tc.qmq.order;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import qunar.tc.qmq.common.OrderedConstants;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * @author zhenwei.liu
 * @since 2019-08-30
 */
public class DefaultOrderedMessageLockManager implements OrderedMessageLockManager {

    private Cache<String, String> lockCache = CacheBuilder.newBuilder()
            .expireAfterWrite(OrderedConstants.ORDERED_CONSUMER_LOCK_LEASE_SECS, TimeUnit.SECONDS)
            .build();

    @Override
    public boolean acquireLock(String subject, String group, String clientId) {
        String key = createLockKey(subject, group);
        synchronized (key.intern()) {
            String oldClient = lockCache.getIfPresent(key);
            if (oldClient == null || Objects.equals(oldClient, clientId)) {
                lockCache.put(key, clientId);
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean releaseLock(String subject, String group, String clientId) {
        String key = createLockKey(subject, group);
        synchronized (key.intern()) {
            String oldClient = lockCache.getIfPresent(key);
            if (oldClient != null && Objects.equals(oldClient, clientId)) {
                lockCache.invalidate(key);
                return true;
            }
        }
        return false;
    }

    private String createLockKey(String subject, String group) {
        return subject + ":" + group;
    }
}
