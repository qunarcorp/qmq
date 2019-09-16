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

package qunar.tc.qmq.consumer.pull;

import qunar.tc.qmq.consumer.register.RegistParam;
import qunar.tc.qmq.protocol.consumer.PullFilter;

import java.util.Collections;
import java.util.List;

/**
 * @author yiqun.fan create on 17-11-2.
 */
public class ConsumeParam {
    private final String subject;
    private final String consumerGroup;
    private final String consumerId;
    private final boolean isBroadcast;
    private final boolean isOrdered;
    private volatile boolean isConsumeMostOnce;
    private final List<PullFilter> filters;

    public ConsumeParam(String subject, String consumerGroup, RegistParam param) {
        this(subject, consumerGroup, param.isBroadcast(), param.isOrdered(), param.isConsumeMostOnce(), param.getClientId(), param.getFilters());
    }

    public ConsumeParam(String subject, String consumerGroup, boolean isBroadcast, boolean isOrdered, boolean isConsumeMostOnce, String clientId) {
        this(subject, consumerGroup, isBroadcast, isOrdered, isConsumeMostOnce, clientId, Collections.<PullFilter>emptyList());
    }

    private ConsumeParam(String subject, String consumerGroup, boolean isBroadcast, boolean isOrdered, boolean isConsumeMostOnce, String clientId, List<PullFilter> filters) {
        this.subject = subject;
        this.consumerGroup = consumerGroup;
        this.isOrdered = isOrdered;
        this.consumerId = clientId;
        this.isBroadcast = isBroadcast;
        this.filters = filters;
        this.isConsumeMostOnce = isConsumeMostOnce;
    }

    public String getSubject() {
        return subject;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public boolean isBroadcast() {
        return isBroadcast;
    }

    public List<PullFilter> getFilters() {
        return filters;
    }

    public boolean isConsumeMostOnce() {
        return isConsumeMostOnce;
    }

    public void setConsumeMostOnce(boolean consumeMostOnce) {
        isConsumeMostOnce = consumeMostOnce;
    }

    public boolean isOrdered() {
        return isOrdered;
    }
}
