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

package qunar.tc.qmq.consumer.register;

import qunar.tc.qmq.MessageListener;
import qunar.tc.qmq.StatusSource;
import qunar.tc.qmq.SubscribeParam;
import qunar.tc.qmq.protocol.consumer.PullFilter;
import qunar.tc.qmq.protocol.consumer.TagPullFilter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

/**
 * @author yiqun.fan create on 17-8-23.
 */
public class RegistParam {
    private final Executor executor;
    private final MessageListener messageListener;
    private final boolean isConsumeMostOnce;
    private final boolean isBroadcast;
    private final boolean isOrdered;
    private final String clientId;
    private final List<PullFilter> filters;

    private volatile StatusSource actionSrc = StatusSource.HEALTHCHECKER;

    public RegistParam(Executor executor, MessageListener messageListener, SubscribeParam subscribeParam, String clientId) {
        this.executor = executor;
        this.messageListener = messageListener;
        this.isConsumeMostOnce = subscribeParam.isConsumeMostOnce();
        this.clientId = clientId;
        this.isBroadcast = subscribeParam.isBroadcast();
        this.isOrdered = subscribeParam.isOrdered();
        final ArrayList<PullFilter> filters = new ArrayList<>();
        filters.add(new TagPullFilter(subscribeParam.getTagType(), subscribeParam.getTags()));
        this.filters = filters;
    }

    public Executor getExecutor() {
        return executor;
    }

    public MessageListener getMessageListener() {
        return messageListener;
    }

    public String getClientId() {
        return this.clientId;
    }

    public boolean isConsumeMostOnce() {
        return isConsumeMostOnce;
    }

    public boolean isBroadcast() {
        return isBroadcast;
    }

    public List<PullFilter> getFilters() {
        return filters;
    }

    public void addFilter(final PullFilter filter) {
        filters.add(filter);
    }

    public void setActionSrc(StatusSource actionSrc) {
        this.actionSrc = actionSrc;
    }

    public StatusSource getActionSrc() {
        return this.actionSrc;
    }

    public boolean isOrdered() {
        return isOrdered;
    }
}
