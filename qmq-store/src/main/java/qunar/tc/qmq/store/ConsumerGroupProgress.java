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

import java.util.Map;

/**
 * @author keli.wang
 * @since 2018/10/24
 */
public class ConsumerGroupProgress {
    private final String partitionName;
    private final String consumerGroup;
    private final Map<String, ConsumerProgress> consumers;
    private boolean exclusiveConsume;
    private long pull;

    public ConsumerGroupProgress(String partitionName, String consumerGroup, boolean isExclusiveConsume, long pull, Map<String, ConsumerProgress> consumers) {
        this.partitionName = partitionName;
        this.consumerGroup = consumerGroup;
        this.exclusiveConsume = isExclusiveConsume;
        this.pull = pull;
        this.consumers = consumers;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public boolean isExclusiveConsume() {
        return exclusiveConsume;
    }

    public Map<String, ConsumerProgress> getConsumers() {
        return consumers;
    }

    public long getPull() {
        return pull;
    }

    public void setPull(long pull) {
        this.pull = pull;
    }
}
