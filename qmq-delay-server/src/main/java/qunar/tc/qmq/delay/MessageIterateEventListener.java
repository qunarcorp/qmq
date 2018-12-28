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

package qunar.tc.qmq.delay;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.delay.base.AppendException;
import qunar.tc.qmq.delay.store.model.AppendLogResult;
import qunar.tc.qmq.delay.store.model.LogRecord;
import qunar.tc.qmq.protocol.producer.MessageProducerCode;

import java.util.function.Function;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-07-19 18:21
 */
public class MessageIterateEventListener implements EventListener<LogRecord> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageIterateEventListener.class);

    private final DelayLogFacade facade;
    private final Function<ScheduleIndex, Boolean> iterateCallback;

    MessageIterateEventListener(final DelayLogFacade facade, Function<ScheduleIndex, Boolean> iterateCallback) {
        this.facade = facade;
        this.iterateCallback = iterateCallback;
    }

    @Override
    public void post(LogRecord event) {
        AppendLogResult<ScheduleIndex> result = facade.appendScheduleLog(event);
        int code = result.getCode();
        if (MessageProducerCode.SUCCESS != code) {
            LOGGER.error("appendMessageLog schedule log error,log:{} {},code:{}", event.getSubject(), event.getMessageId(), code);
            throw new AppendException("appendScheduleLogError");
        }

        iterateCallback.apply(result.getAdditional());
    }
}
