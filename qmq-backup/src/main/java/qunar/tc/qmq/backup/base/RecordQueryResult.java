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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019-03-05 15:25
 */
public class RecordQueryResult implements Serializable {
    private static final long serialVersionUID = -6489077654100402117L;

    private final List<Record> records;

    @JsonCreator
    public RecordQueryResult(@JsonProperty("records") List<Record> records) {
        this.records = records;
    }

    public List<Record> getRecords() {
        return records;
    }

    public static class Record {
        private final String consumerGroup;
        private final byte action;
        private final byte type;
        private final long timestamp;
        private final String consumerId;
        private final long sequence;

        public Record(String consumerGroup, byte action, byte type, long timestamp, String consumerId, long sequence) {
            this.consumerGroup = consumerGroup;
            this.action = action;
            this.type = type;
            this.timestamp = timestamp;
            this.consumerId = consumerId;
            this.sequence = sequence;
        }

        public String getConsumerGroup() {
            return consumerGroup;
        }

        public byte getAction() {
            return action;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public String getConsumerId() {
            return consumerId;
        }

        public long getSequence() {
            return sequence;
        }

        public byte getType() {
            return type;
        }
    }

}
