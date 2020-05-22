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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author keli.wang
 * @since 2017/8/21
 */
public class MaxAckedPullLogSequence {
    private final String subject;
    private final String group;
    private final String consumerId;

    private final AtomicLong maxSequence;

    @JsonCreator
    public MaxAckedPullLogSequence(@JsonProperty("subject") String subject,
                                   @JsonProperty("group") String group,
                                   @JsonProperty("consumerId") String consumerId,
                                   @JsonProperty("maxSequence") long maxSequence) {
        this.subject = subject;
        this.group = group;
        this.consumerId = consumerId;
        this.maxSequence = new AtomicLong(maxSequence);
    }

    public String getSubject() {
        return subject;
    }

    public String getGroup() {
        return group;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public long getMaxSequence() {
        return maxSequence.get();
    }

    public void setMaxSequence(final long maxSequence) {
        this.maxSequence.set(maxSequence);
    }

    @Override
    public String toString() {
        return "MaxAckedPullLogSequence{" +
                "subject='" + subject + '\'' +
                ", group='" + group + '\'' +
                ", consumerId='" + consumerId + '\'' +
                ", maxSequence=" + maxSequence +
                '}';
    }
}
