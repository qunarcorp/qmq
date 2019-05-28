package qunar.tc.qmq.backup.service;

import qunar.tc.qmq.store.LogVisitorRecord;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-11-27 17:28
 */
public interface SyncLogIterator<T,P> {
    boolean hasNext(P buf);

    LogVisitorRecord<T> next(P buf);
}
