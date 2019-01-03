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

package qunar.tc.qmq.delay.store.log;

import qunar.tc.qmq.delay.cleaner.LogCleaner;
import qunar.tc.qmq.delay.config.StoreConfiguration;
import qunar.tc.qmq.delay.store.DefaultDelaySegmentValidator;
import qunar.tc.qmq.delay.store.appender.DispatchLogAppender;
import qunar.tc.qmq.store.PeriodicFlushService;
import qunar.tc.qmq.store.SegmentBuffer;
import qunar.tc.qmq.sync.DelaySyncRequest;

import java.io.File;
import java.nio.ByteBuffer;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-07-19 16:36
 */
public class DispatchLog extends AbstractDelayLog<Boolean> {
    /**
     * message log flush interval,500ms
     */
    private static final int DEFAULT_FLUSH_INTERVAL = 500;

    public DispatchLog(StoreConfiguration storeConfiguration) {
        super(new DispatchLogSegmentContainer(storeConfiguration,
                new File(storeConfiguration.getDispatchLogStorePath())
                , new DefaultDelaySegmentValidator(), new DispatchLogAppender()));
    }

    public PeriodicFlushService.FlushProvider getProvider() {
        return new PeriodicFlushService.FlushProvider() {
            @Override
            public int getInterval() {
                return DEFAULT_FLUSH_INTERVAL;
            }

            @Override
            public void flush() {
                DispatchLog.this.flush();
            }
        };
    }

    public DispatchLogSegment latestSegment() {
        return ((DispatchLogSegmentContainer) container).latestSegment();
    }

    public void clean(LogCleaner.CleanHook hook) {
        ((DispatchLogSegmentContainer) container).clean(hook);
    }

    public SegmentBuffer getDispatchLogData(long segmentBaseOffset, long dispatchLogOffset) {
        return ((DispatchLogSegmentContainer) container).getDispatchData(segmentBaseOffset, dispatchLogOffset);
    }

    public long getMaxOffset(long dispatchSegmentBaseOffset) {
        return ((DispatchLogSegmentContainer) container).getMaxOffset(dispatchSegmentBaseOffset);
    }

    public DelaySyncRequest.DispatchLogSyncRequest getSyncMaxRequest() {
        return ((DispatchLogSegmentContainer) container).getSyncMaxRequest();
    }

    public boolean appendData(long startOffset, long baseOffset, ByteBuffer body) {
        return ((DispatchLogSegmentContainer) container).appendData(startOffset, baseOffset, body);
    }

    public DispatchLogSegment lowerSegment(long latestOffset) {
        return ((DispatchLogSegmentContainer) container).lowerSegment(latestOffset);
    }

    public long higherBaseOffset(long low) {
        return ((DispatchLogSegmentContainer) container).higherBaseOffset(low);
    }
}
