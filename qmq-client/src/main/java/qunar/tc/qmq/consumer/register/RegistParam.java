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
import qunar.tc.qmq.SubscribeParam;
import qunar.tc.qmq.common.StatusSource;

import java.util.concurrent.Executor;

/**
 * @author yiqun.fan create on 17-8-23.
 */
public class RegistParam {
    private final Executor executor;
    private final MessageListener messageListener;
    private final SubscribeParam subscribeParam;
    private boolean isBroadcast = false;
    private final String clientId;

    private volatile StatusSource actionSrc = StatusSource.HEALTHCHECKER;

    public RegistParam(Executor executor, MessageListener messageListener, SubscribeParam subscribeParam, String clientId) {
        this.executor = executor;
        this.messageListener = messageListener;
        this.subscribeParam = subscribeParam;
        this.clientId = clientId;
    }

    public Executor getExecutor() {
        return executor;
    }

    public MessageListener getMessageListener() {
        return messageListener;
    }

    public SubscribeParam getSubscribeParam() {
        return subscribeParam;
    }

    public String getClientId() {
        return this.clientId;
    }

    public boolean isBroadcast() {
        return isBroadcast;
    }

    public void setBroadcast(boolean broadcast) {
        isBroadcast = broadcast;
    }

    public void setActionSrc(StatusSource actionSrc) {
        this.actionSrc = actionSrc;
    }

    public StatusSource getActionSrc() {
        return this.actionSrc;
    }
}
