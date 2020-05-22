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

package qunar.tc.qmq.backup.sync;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.backup.base.ActionRecord;
import qunar.tc.qmq.backup.base.ScheduleFlushable;
import qunar.tc.qmq.backup.service.BatchBackup;
import qunar.tc.qmq.backup.service.SyncLogIterator;
import qunar.tc.qmq.base.SyncRequest;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.store.*;
import qunar.tc.qmq.sync.AbstractSyncLogProcessor;
import qunar.tc.qmq.sync.SyncType;

import static qunar.tc.qmq.backup.config.DefaultBackupConfig.DEFAULT_FLUSH_INTERVAL;
import static qunar.tc.qmq.backup.config.DefaultBackupConfig.SYNC_OFFSET_FLUSH_INTERVAL_CONFIG_KEY;
import static qunar.tc.qmq.backup.service.impl.ActionSyncLogIterator.BLANK_ACTION;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-12-06 18:35
 */
public class BackupActionLogSyncProcessor extends AbstractSyncLogProcessor implements ScheduleFlushable {
    private static final Logger LOG = LoggerFactory.getLogger(BackupActionLogSyncProcessor.class);

    private final PeriodicFlushService flushService;
    private final CheckpointManager checkpointManager;
    private final SyncLogIterator<Action, ByteBuf> iterator;
    private final BatchBackup<ActionRecord> recordBackup;

    public BackupActionLogSyncProcessor(CheckpointManager checkpointManager, DynamicConfig config, SyncLogIterator<Action, ByteBuf> iterator
            , BatchBackup<ActionRecord> recordBackup) {
        this.checkpointManager = checkpointManager;
        this.iterator = iterator;
        this.recordBackup = recordBackup;
        PeriodicFlushService.FlushProvider provider = new PeriodicFlushService.FlushProvider() {
            @Override
            public int getInterval() {
                return config.getInt(SYNC_OFFSET_FLUSH_INTERVAL_CONFIG_KEY, DEFAULT_FLUSH_INTERVAL);
            }

            @Override
            public void flush() {
                BackupActionLogSyncProcessor.this.flush();
            }
        };
        this.flushService = new PeriodicFlushService(provider);
    }

    @Override
    public void appendLogs(long startOffset, ByteBuf body) {
        checkpointManager.setSyncActionLogOffset(startOffset);
        while (iterator.hasNext(body)) {
            final int start = body.readerIndex();
            LogVisitorRecord<Action> record = iterator.next(body);
            if (record.isNoMore()) break;
            if (!record.hasData()) {
                long relativePosition = checkpointManager.getSyncActionLogOffset() % ActionLog.PER_SEGMENT_FILE_SIZE;
                checkpointManager.addSyncActionLogOffset((int) (ActionLog.PER_SEGMENT_FILE_SIZE - relativePosition));
                continue;
            }
            Action action = record.getData();
            if (action.equals(BLANK_ACTION)) {
                checkpointManager.addSyncActionLogOffset(body.readerIndex() - start);
                continue;
            }
            recordBackup.add(new ActionRecord(action), null);
            checkpointManager.addSyncActionLogOffset(body.readerIndex() - start);
        }
    }

    @Override
    public void scheduleFlush() {
        flushService.start();
    }

    @Override
    public void flush() {
        checkpointManager.saveSyncActionCheckpointSnapshot();
    }

    @Override
    public SyncRequest getRequest() {
        final long actionLogOffset = checkpointManager.getSyncActionLogOffset();
        return new SyncRequest(SyncType.action.getCode(), actionLogOffset, actionLogOffset);
    }

    @Override
    public void destroy() {
        flushService.close();
    }
}
