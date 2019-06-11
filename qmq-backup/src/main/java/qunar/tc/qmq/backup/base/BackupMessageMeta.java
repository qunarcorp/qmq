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
import java.util.Objects;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-12-26 17:05
 */
public class BackupMessageMeta implements Serializable {
    private static final long serialVersionUID = 3731878088375518497L;

    private final long sequence;
    private final String brokerGroup;
    private final String messageId;

    private long createTime;
    private String consumerGroupId;

    public BackupMessageMeta(long sequence, String brokerGroup, String messageId) {
        this.sequence = sequence;
        this.brokerGroup = brokerGroup;
        this.messageId = messageId;
    }

    public BackupMessageMeta(long sequence, String brokerGroup) {
        this(sequence, brokerGroup, "");
    }

    public String getMessageId() {
        return messageId;
    }

    public long getSequence() {
        return sequence;
    }

    public String getBrokerGroup() {
        return brokerGroup;
    }

    public String getConsumerGroupId() {
        return consumerGroupId;
    }

    public void setConsumerGroupId(String consumerGroupId) {
        this.consumerGroupId = consumerGroupId;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BackupMessageMeta that = (BackupMessageMeta) o;
        return sequence == that.sequence &&
                Objects.equals(brokerGroup, that.brokerGroup);
    }

    @Override
    public int hashCode() {

        return Objects.hash(sequence, brokerGroup);
    }

}
