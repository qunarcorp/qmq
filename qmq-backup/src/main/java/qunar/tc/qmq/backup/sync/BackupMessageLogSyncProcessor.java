package qunar.tc.qmq.backup.sync;

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.base.SyncRequest;
import qunar.tc.qmq.sync.AbstractSyncLogProcessor;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-11-27 16:30
 */
public class BackupMessageLogSyncProcessor extends AbstractSyncLogProcessor {
    private final LogSyncDispatcher dispatcher;

    public BackupMessageLogSyncProcessor(final LogSyncDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override
    public void appendLogs(long startOffset, ByteBuf body) {
        dispatcher.dispatch(startOffset, body);
    }

    @Override
    public SyncRequest getRequest() {
        return new SyncRequest(dispatcher.getSyncType().getCode(), dispatcher.getSyncLogOffset(), -1L);
    }
}
