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

package qunar.tc.qmq.backup.base;

import java.io.Serializable;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019-03-05 15:24
 */
public class RecordQuery implements Serializable {
    private static final long serialVersionUID = 3494784604691443324L;

    private String subject;

    private long sequence;

    private String brokerGroup;

    private String messageId;

    private byte recordCode;

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public long getSequence() {
        return sequence;
    }

    public void setSequence(long sequence) {
        this.sequence = sequence;
    }

    public String getBrokerGroup() {
        return brokerGroup;
    }

    public void setBrokerGroup(String brokerGroup) {
        this.brokerGroup = brokerGroup;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public byte getRecordCode() {
        return recordCode;
    }

    public void setRecordCode(byte recordCode) {
        this.recordCode = recordCode;
    }
}
