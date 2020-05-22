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
    private final String subject;
    private final String group;
    private final Map<String, ConsumerProgress> consumers;
    // TODO(keli.wang): mark broadcast as final after new snapshot file created
    private boolean broadcast;
    private long pull;

    public ConsumerGroupProgress(String subject, String group, boolean broadcast, long pull, Map<String, ConsumerProgress> consumers) {
        this.subject = subject;
        this.group = group;
        this.broadcast = broadcast;
        this.pull = pull;
        this.consumers = consumers;
    }

    public String getSubject() {
        return subject;
    }

    public String getGroup() {
        return group;
    }

    public boolean isBroadcast() {
        return broadcast;
    }

    public void setBroadcast(boolean broadcast) {
        this.broadcast = broadcast;
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
