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

package qunar.tc.qmq.store;

/**
 * Created by zhaohui.yu
 * 3/13/19
 */
public class MessageQueryIndex implements Comparable<MessageQueryIndex> {
    private final String subject;

    private final String messageId;

    private final long createTime;

    private final long sequence;

    private transient int backupRetryTimes = 0;

    private transient long currentOffset;

    MessageQueryIndex(String subject, String messageId, long sequence, long createTime) {
        this.subject = subject;
        this.messageId = messageId;
        this.sequence = sequence;
        this.createTime = createTime;
    }

    public String getSubject() {
        return subject;
    }

    public String getMessageId() {
        return messageId;
    }

    public long getSequence() {
        return sequence;
    }

    public long getCreateTime() {
        return createTime;
    }

    public int getBackupRetryTimes() {
        return backupRetryTimes;
    }

    public void setBackupRetryTimes(int backupRetryTimes) {
        this.backupRetryTimes = backupRetryTimes;
    }

    public void setCurrentOffset(long currentOffset) {
        this.currentOffset = currentOffset;
    }

    public long getCurrentOffset() {
        return currentOffset;
    }

    @Override
    public int compareTo(MessageQueryIndex o) {
        return Long.compare(currentOffset, o.getCurrentOffset());
    }

    @Override
    public int hashCode() {
        return Long.hashCode(currentOffset);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof MessageQueryIndex)) return false;
        return currentOffset == ((MessageQueryIndex) obj).currentOffset;
    }
}
