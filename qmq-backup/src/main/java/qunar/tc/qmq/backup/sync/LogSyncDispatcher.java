package qunar.tc.qmq.backup.sync;

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.sync.SyncType;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-11-27 16:58
 */
public interface LogSyncDispatcher {
    void dispatch(long startOffset, ByteBuf body);

    long getSyncLogOffset();

    SyncType getSyncType();
}
