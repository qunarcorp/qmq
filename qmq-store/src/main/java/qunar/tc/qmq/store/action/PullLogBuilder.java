/*
 * Copyright 2018 Qunar, Inc.
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

package qunar.tc.qmq.store.action;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.store.MemTableManager;
import qunar.tc.qmq.store.PullLogMemTable;
import qunar.tc.qmq.store.PullLogMessage;
import qunar.tc.qmq.store.StorageConfig;
import qunar.tc.qmq.store.event.FixedExecOrderEventBus;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author keli.wang
 * @since 2017/8/21
 */
public class PullLogBuilder implements FixedExecOrderEventBus.Listener<ActionEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(PullLogBuilder.class);

    private final StorageConfig config;
    private final MemTableManager manager;
    private volatile PullLogMemTable currentMemTable;

    public PullLogBuilder(final StorageConfig config, final MemTableManager manager) {
        this.config = config;
        this.manager = manager;
    }

    @Override
    public void onEvent(final ActionEvent event) {
        switch (event.getAction().type()) {
            case PULL:
                buildPullLog(event);
                break;
            case RANGE_ACK:
                ack(event);
                break;
        }
    }

    private void ack(ActionEvent event) {
        final RangeAckAction action = (RangeAckAction) event.getAction();
        PullLogMemTable memTable = this.currentMemTable;
        if (memTable == null) return;

        memTable.ack(action.subject(), action.group(), action.consumerId(), action.getFirstSequence(), action.getLastSequence());
    }

    private void buildPullLog(ActionEvent event) {
        final PullAction action = (PullAction) event.getAction();
        if (action.isBroadcast()) return;

        if (action.getFirstSequence() - action.getLastSequence() > 0) return;

        List<PullLogMessage> messages = createMessages(action);
        if (needRolling(messages)) {
            final long nextTabletId = event.getOffset();
            LOG.info("rolling new pull log memtable, nextTabletId: {}, event: {}", nextTabletId, event);

            currentMemTable = (PullLogMemTable) manager.rollingNewMemTable(nextTabletId, event.getOffset());
        }

        if (currentMemTable == null) {
            throw new RuntimeException("lost first event of current log segment");
        }

        currentMemTable.putPullLogMessages(
                action.subject(),
                action.group(),
                action.consumerId(),
                messages);
        currentMemTable.setEndOffset(event.getOffset());

        blockIfTooMuchActiveMemTable();
    }

    private boolean needRolling(final List<PullLogMessage> messages) {
        if (currentMemTable == null) {
            return true;
        }

        int wroteBytes = messages.size() * PullLogMemTable.ENTRY_SIZE;
        return !currentMemTable.checkWritable(wroteBytes);
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

    private List<PullLogMessage> createMessages(final PullAction action) {
        final int count = (int) (action.getLastSequence() - action.getFirstSequence() + 1);
        final List<PullLogMessage> messages = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            messages.add(new PullLogMessage(action.getFirstSequence() + i, action.getFirstMessageSequence() + i));
        }

        return messages;
    }
}
