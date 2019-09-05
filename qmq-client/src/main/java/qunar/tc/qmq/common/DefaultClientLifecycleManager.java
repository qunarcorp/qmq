package qunar.tc.qmq.common;

import com.google.common.base.Joiner;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;

import java.util.concurrent.TimeUnit;

/**
 * @author zhenwei.liu
 * @since 2019-09-05
 */
public class DefaultClientLifecycleManager implements ClientLifecycleManager {

    private static final Joiner keyJoiner = Joiner.on(":");
    private ExpiringMap<String, Integer> cache = ExpiringMap.create();

    @Override
    public boolean isAlive(String subject, String group, int physicalPartition) {
        String key = createKey(subject, group, physicalPartition);
        Integer oldVersion = cache.get(key);
        return oldVersion != null;
    }

    @Override
    public boolean refreshLifecycle(String subject, String group, int physicalPartition, int version, long expired) {
        String key = createKey(subject, group, physicalPartition);
        long expireMills = expired - System.currentTimeMillis();
        synchronized (key.intern()) {
            Integer oldVersion = cache.get(key);
            if (oldVersion == null || oldVersion <= version) {
                cache.put(key, version, ExpirationPolicy.CREATED, expireMills, TimeUnit.MILLISECONDS);
                return true;
            }
        }
        return false;
    }

    private static String createKey(String subject, String group, int physicalPartition) {
        return keyJoiner.join(subject, group, physicalPartition);
    }
}
