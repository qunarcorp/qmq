/*
 * Copyright 2019 Qunar, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package qunar.tc.qmq.store;

import com.google.common.base.CharMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.store.event.FixedExecOrderEventBus;
import qunar.tc.qmq.store.result.Result;

import java.util.concurrent.TimeUnit;

/**
 * @author keli.wang
 * @since 2019-06-10
 */
public class BuildMessageMemTableEventListener implements FixedExecOrderEventBus.Listener<MessageLogRecord> {
    private static final Logger LOG = LoggerFactory.getLogger(BuildMessageMemTableEventListener.class);

    private final StorageConfig config;
    private final MessageMemTableManager manager;
    private final SortedMessagesTable smt;

    private volatile MessageMemTable currentMemTable;
    private volatile long tabletId;

    public BuildMessageMemTableEventListener(final StorageConfig config, final MessageMemTableManager manager, final SortedMessagesTable smt) {
        this.config = config;
        this.manager = manager;
        this.smt = smt;
        this.tabletId = -1;
    }

    @Override
    public void onEvent(final MessageLogRecord event) {
        if (CharMatcher.INVISIBLE.matchesAnyOf(event.getSubject())) {
            LOG.error("hit illegal subject during iterate message log, skip this message. subject: {}, wroteOffset: {}", event.getSubject(), event.getWroteOffset());
            return;
        }

        if (needRolling(event)) {
            final long nextTabletId = smt.getNextTabletId(tabletId);
            LOG.info("rolling new memtable, nextTabletId: {}, event: {}", nextTabletId, event);

            currentMemTable = manager.rollingNewMemTable(nextTabletId, event.getWroteOffset());
            tabletId = nextTabletId;
        }

        if (currentMemTable == null) {
            throw new RuntimeException("lost first event of current log segment");
        }

        manager.updateMaxMessageSequence(event.getSubject(), event.getSequence());

        final long offset = event.getWroteOffset() + event.getWroteBytes();
        final Result<MessageMemTable.AddResultStatus, MessageMemTable.MessageIndex> result = currentMemTable.add(
                event.getSubject(),
                event.getSequence(),
                offset,
                event.getPayload());
        switch (result.getStatus()) {
            case SUCCESS:
                break;
            case OVERFLOW:
                throw new RuntimeException("memtable overflow");
            default:
                throw new RuntimeException("unknown status " + result.getStatus());
        }

        blockIfTooMuchActiveMemTable();
    }

    private boolean needRolling(final MessageLogRecord event) {
        if (currentMemTable == null) {
            return true;
        }

        return !currentMemTable.checkWritable(event.getWroteBytes());
    }

    private void blockIfTooMuchActiveMemTable() {
        while (manager.getActiveCount() > config.getMaxActiveMemTable()) {
            try {
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException ignore) {
                LOG.error("sleep interrupted");
            }
        }
    }
}