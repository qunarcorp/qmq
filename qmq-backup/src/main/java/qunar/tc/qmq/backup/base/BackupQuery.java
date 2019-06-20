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
import java.util.Date;

/**
 * 备份消息查询参数类
 *
 * @author kelly.li
 * @date 2014-03-24
 */
public class BackupQuery implements Serializable {
    private static final long serialVersionUID = 2082285700951795679L;

    /**
     * 消息创建起始时间
     */
    private Date msgCreateTimeBegin;
    /**
     * 消息创建结束时间
     */
    private Date msgCreateTimeEnd;
    /**
     * 消息主题
     */
    private String subject;
    /**
     * 消息id
     */
    private String messageId;
    /**
     * 是否是delay消息查询标识
     */
    private boolean isDelay;

    private String brokerGroup;

    private long startOffset;

    private int size;

    private String consumerGroup;

    private long sequence;

    private Serializable start;

    private int len;

    public Date getMsgCreateTimeBegin() {
        return msgCreateTimeBegin;
    }

    public void setMsgCreateTimeBegin(Date msgCreateTimeBegin) {
        this.msgCreateTimeBegin = msgCreateTimeBegin;
    }

    public Date getMsgCreateTimeEnd() {
        return msgCreateTimeEnd;
    }

    public void setMsgCreateTimeEnd(Date msgCreateTimeEnd) {
        this.msgCreateTimeEnd = msgCreateTimeEnd;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public boolean isDelay() {
        return isDelay;
    }

    public void setDelay(boolean isDelay) {
        this.isDelay = isDelay;
    }

    public String getBrokerGroup() {
        return brokerGroup;
    }

    public void setBrokerGroup(String brokerGroup) {
        this.brokerGroup = brokerGroup;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(long startOffset) {
        this.startOffset = startOffset;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public long getSequence() {
        return sequence;
    }

    public void setSequence(long sequence) {
        this.sequence = sequence;
    }

    public Serializable getStart() {
        return start;
    }

    public void setStart(Serializable start) {
        this.start = start;
    }

    public int getLen() {
        return len;
    }

    public void setLen(int len) {
        this.len = len;
    }

    @Override
    public String toString() {
        return "BackupQuery{" +
                "msgCreateTimeBegin=" + msgCreateTimeBegin +
                ", msgCreateTimeEnd=" + msgCreateTimeEnd +
                ", subject='" + subject + '\'' +
                ", messageId='" + messageId + '\'' +
                ", isDelay=" + isDelay +
                ", brokerGroup='" + brokerGroup + '\'' +
                ", startOffset=" + startOffset +
                ", size=" + size +
                ", consumerGroup='" + consumerGroup + '\'' +
                ", sequence=" + sequence +
                ", start=" + start +
                ", len=" + len +
                '}';
    }
}
