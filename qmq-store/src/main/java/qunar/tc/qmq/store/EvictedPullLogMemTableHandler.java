package qunar.tc.qmq.store;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import qunar.tc.qmq.concurrent.NamedThreadFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static qunar.tc.qmq.store.MemTableManager.MemTableEvictedCallback;

/**
 * Created by zhaohui.yu
 * 2020/6/7
 */
class EvictedPullLogMemTableHandler implements MemTableEvictedCallback {

    private final SortedPullLogTable smt;
    private final CheckpointManager checkpointManager;
    private final ExecutorService flushExecutor;

    public EvictedPullLogMemTableHandler(final SortedPullLogTable sortedPullLogTable, final CheckpointManager checkpointManager) {
        this.smt = sortedPullLogTable;
        this.checkpointManager = checkpointManager;
        this.flushExecutor = Executors.newSingleThreadExecutor(new NamedThreadFactory("pulllog-smt-flusher"));
    }

    @Override
    public boolean onEvicted(MemTable table) {
        try {
            return smt.newTabletBuilder(table.getTabletId())
                    .map(builder -> buildTablet(builder, (PullLogMemTable) table))
                    .orElse(false);
        } catch (IOException e) {
            return false;
        }
    }

    private boolean buildTablet(final SortedPullLogTable.TabletBuilder builder, final PullLogMemTable table) {
        if (!builder.begin(table.getBeginOffset(), table.getEndOffset())) {
            return false;
        }

        final ByteBuf buffer = ByteBufAllocator.DEFAULT.ioBuffer(table.getCapacity());
        try {
            Map<String, PullLogIndexEntry> indexMap = new HashMap<>();
            table.dump(buffer, indexMap);
            fillBlank(buffer, table.getCapacity());
            builder.append(buffer.nioBuffer());
            builder.appendIndex(indexMap);
            final boolean ok = builder.finish();
            if (!ok) {
                return false;
            } else {
                flushExecutor.submit(() -> {
                    builder.flush();
                    //这里可能有点问题，一个pull log table非常紧凑，如果写完一个table才保存action的checkpoint，会导致重启后大量的action log回放
                    updateCheckpoint(table);
                });
                return true;
            }
        } finally {
            ReferenceCountUtil.safeRelease(buffer);
        }
    }

    private void fillBlank(ByteBuf buffer, int capacity) {
        int position = buffer.writerIndex();
        if (position == capacity) return;

        for (int i = 0; i < capacity - position; ++i) {
            buffer.writeByte(0);
        }
    }

    private void updateCheckpoint(final MemTable table) {
        checkpointManager.updateActionCheckpoint(table.getEndOffset());
        final Snapshot<ActionCheckpoint> snapshot = checkpointManager.createActionCheckpointSnapshot();
        checkpointManager.saveActionCheckpointSnapshot(snapshot);
    }
}
