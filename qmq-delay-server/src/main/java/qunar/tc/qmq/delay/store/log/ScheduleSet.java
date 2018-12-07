/*
 * Copyright 2018 Qunar
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
 * limitations under the License.com.qunar.pay.trade.api.card.service.usercard.UserCardQueryFacade
 */

package qunar.tc.qmq.delay.store.log;

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.delay.ScheduleIndex;
import qunar.tc.qmq.delay.store.model.LogRecord;
import qunar.tc.qmq.delay.store.model.RecordResult;
import qunar.tc.qmq.delay.store.model.ScheduleSetRecord;
import qunar.tc.qmq.delay.store.model.ScheduleSetSequence;

import java.util.Map;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-07-19 13:50
 */
public class ScheduleSet extends AbstractDelayLog<ScheduleSetSequence> {

    public ScheduleSet(SegmentContainer<RecordResult<ScheduleSetSequence>, LogRecord> container) {
        super(container);
    }

    ScheduleSetRecord recoverRecord(ByteBuf index) {
        long scheduleTime = ScheduleIndex.scheduleTime(index);
        long offset = ScheduleIndex.offset(index);
        int size = ScheduleIndex.size(index);
        return ((ScheduleSetSegmentContainer) container).recover(scheduleTime, size, offset);
    }

    public void clean() {
        ((ScheduleSetSegmentContainer) container).clean();
    }

    public ScheduleSetSegment loadSegment(int segmentBaseOffset) {
        return ((ScheduleSetSegmentContainer) container).loadSegment(segmentBaseOffset);
    }

    synchronized Map<Integer, Long> countSegments() {
        return ((ScheduleSetSegmentContainer) container).countSegments();
    }

    void reValidate(final Map<Integer, Long> offsets, int singleMessageLimitSize) {
        ((ScheduleSetSegmentContainer) container).reValidate(offsets, singleMessageLimitSize);
    }

    int higherBaseOffset(int low) {
        return ((ScheduleSetSegmentContainer) container).higherBaseOffset(low);
    }
}
