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

package qunar.tc.qmq.delay.store.model;

import qunar.tc.qmq.base.MessageHeader;


/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-07-14 11:14
 */
public class LogRecordHeader {
    private MessageHeader header;
    private long scheduleTime;
    private long sequence;

    public LogRecordHeader(String subject, String messageId, long scheduleTime, long sequence) {
        this.header = new MessageHeader();
        this.header.setSubject(subject);
        this.header.setMessageId(messageId);
        this.scheduleTime = scheduleTime;
        this.sequence = sequence;
    }

    public String getSubject() {
        return header.getSubject();
    }

    public String getMessageId() {
        return header.getMessageId();
    }

    public long getScheduleTime() {
        return scheduleTime;
    }

    public long getSequence() {
        return sequence;
    }

    @Override
    public String toString() {
        return "LogRecordHeader{" +
                "header=" + header +
                ", scheduleTime=" + scheduleTime +
                ", sequence=" + sequence +
                '}';
    }
}
