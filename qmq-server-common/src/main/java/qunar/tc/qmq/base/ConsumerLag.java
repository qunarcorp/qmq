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

package qunar.tc.qmq.base;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author keli.wang
 * @since 2018/7/31
 */
public class ConsumerLag {
    private final long pull;
    private final long ack;

    @JsonCreator
    public ConsumerLag(@JsonProperty("pull") final long pull, @JsonProperty("ack") final long ack) {
        this.pull = pull;
        this.ack = ack;
    }

    public long getPull() {
        return pull;
    }

    public long getAck() {
        return ack;
    }
}
