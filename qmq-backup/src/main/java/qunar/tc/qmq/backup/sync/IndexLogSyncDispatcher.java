package qunar.tc.qmq.backup.sync;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.store.IndexLog;
import qunar.tc.qmq.sync.SyncType;

import java.util.concurrent.TimeUnit;

public class IndexLogSyncDispatcher implements LogSyncDispatcher {
    private static final Logger LOG = LoggerFactory.getLogger(IndexLogSyncDispatcher.class);

    private final IndexLog log;

    public IndexLogSyncDispatcher(IndexLog log) {
        this.log = log;
    }

    @Override
    public void dispatch(long startOffset, ByteBuf body) {
        final long currentTime = System.currentTimeMillis();
        try {
            log.appendData(startOffset, body);
        } catch (Exception e) {
            LOG.error("index log dispatch failed.", e);
            Metrics.counter("indexDispatchError").inc();
        } finally {
            Metrics.timer("indexLogDispatcher").update(System.currentTimeMillis() - currentTime, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public long getSyncLogOffset() {
        return log.getMessageOffset();
    }

    @Override
    public SyncType getSyncType() {
        return SyncType.index;
    }
}
