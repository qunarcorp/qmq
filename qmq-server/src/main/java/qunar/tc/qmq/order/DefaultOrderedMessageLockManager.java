package qunar.tc.qmq.order;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Objects;

/**
 * @author zhenwei.liu
 * @since 2019-08-30
 */
public class DefaultOrderedMessageLockManager implements OrderedMessageLockManager {

    private static class Lock {
        private String clientId;
        private int version;

        public String getClientId() {
            return clientId;
        }

        public Lock setClientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public int getVersion() {
            return version;
        }

        public Lock setVersion(int version) {
            this.version = version;
            return this;
        }
    }

    private Map<String, Lock> lockMap = Maps.newConcurrentMap();

    @Override
    public boolean acquireLock(String subject, String group, String clientId, int version) {
        String key = createLockKey(subject, group);
        synchronized (key.intern()) {
            Lock oldLock = lockMap.get(key);
            if (oldLock == null) {
                Lock lock = new Lock();
                lock.setClientId(clientId);
                lock.setVersion(version);
                lockMap.put(key, lock);
                return true;
            } else if (Objects.equals(oldLock.getClientId(), clientId)) {
                int oldVersion = oldLock.getVersion();
                if (oldVersion < version) {
                    oldLock.setVersion(version);
                }
                return true;
            } else {
                // 已有其他 client 锁定, 比较版本
                int oldVersion = oldLock.getVersion();
                if (oldVersion < version) {
                    Lock lock = new Lock();
                    lock.setClientId(clientId);
                    lock.setVersion(version);
                    lockMap.put(key, lock);
                    return true;
                } else if (oldVersion == version) {
                    throw new IllegalStateException(String.format("subject %s group %s 找到两个版本相同的分配 client old %s current %s version %s",
                            subject, group, oldLock.getClientId(), clientId, version));
                } else {
                    return false;
                }
            }
        }
    }

    @Override
    public boolean releaseLock(String subject, String group, String clientId) {
        String key = createLockKey(subject, group);
        synchronized (key.intern()) {
            Lock oldLock = lockMap.get(key);
            if (oldLock != null && Objects.equals(oldLock.getClientId(), clientId)) {
                lockMap.remove(key);
                return true;
            }
        }
        return false;
    }

    private String createLockKey(String subject, String group) {
        return subject + ":" + group;
    }
}
