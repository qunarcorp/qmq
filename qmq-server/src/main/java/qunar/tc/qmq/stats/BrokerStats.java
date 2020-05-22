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

package qunar.tc.qmq.stats;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author keli.wang
 * @since 2018/7/18
 */
public class BrokerStats {
    private static final BrokerStats STATS = new BrokerStats();

    private final AtomicLong activeConnectionCount = new AtomicLong(0);

    private final PerMinuteDeltaCounter lastMinuteSendRequestCount = new PerMinuteDeltaCounter();
    private final PerMinuteDeltaCounter lastMinutePullRequestCount = new PerMinuteDeltaCounter();
    private final PerMinuteDeltaCounter lastMinuteAckRequestCount = new PerMinuteDeltaCounter();

    private BrokerStats() {
    }

    public static BrokerStats getInstance() {
        return STATS;
    }

    public AtomicLong getActiveClientConnectionCount() {
        return activeConnectionCount;
    }

    public PerMinuteDeltaCounter getLastMinuteSendRequestCount() {
        return lastMinuteSendRequestCount;
    }

    public PerMinuteDeltaCounter getLastMinutePullRequestCount() {
        return lastMinutePullRequestCount;
    }

    public PerMinuteDeltaCounter getLastMinuteAckRequestCount() {
        return lastMinuteAckRequestCount;
    }
}
