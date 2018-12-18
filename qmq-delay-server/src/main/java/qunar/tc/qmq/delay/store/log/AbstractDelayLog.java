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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.delay.store.model.AppendLogResult;
import qunar.tc.qmq.delay.store.model.LogRecord;
import qunar.tc.qmq.delay.store.model.RecordResult;
import qunar.tc.qmq.protocol.producer.MessageProducerCode;
import qunar.tc.qmq.store.PutMessageStatus;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-07-19 12:28
 */
public abstract class AbstractDelayLog<T> implements Log<RecordResult<T>, LogRecord> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDelayLog.class);

    SegmentContainer<RecordResult<T>, LogRecord> container;

    AbstractDelayLog(SegmentContainer<RecordResult<T>, LogRecord> container) {
        this.container = container;
    }

    @Override
    public AppendLogResult<RecordResult<T>> append(LogRecord record) {
        String subject = record.getSubject();
        RecordResult<T> result = container.append(record);
        PutMessageStatus status = result.getStatus();
        if (PutMessageStatus.SUCCESS != status) {
            LOGGER.error("appendMessageLog schedule set file error,subject:{},status:{}", subject, status.name());
            return new AppendLogResult<>(MessageProducerCode.STORE_ERROR, status.name(), null);
        }

        return new AppendLogResult<>(MessageProducerCode.SUCCESS, status.name(), result);
    }

    @Override
    public boolean clean(Long key) {
        return container.clean(key);
    }

    @Override
    public void flush() {
        container.flush();
    }

}
