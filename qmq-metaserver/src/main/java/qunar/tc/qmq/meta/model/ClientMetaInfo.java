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

package qunar.tc.qmq.meta.model;

import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.base.OnOfflineState;

import java.util.Objects;

/**
 * @author yunfeng.yang
 * @since 2017/9/25
 */
public class ClientMetaInfo {
    private String subject;
    private int clientTypeCode;
    private String appCode;
    private String room;
    private String clientId;
    private String consumerGroup;
    private OnOfflineState onlineStatus;
    private ConsumeStrategy consumeStrategy;

    public ClientMetaInfo(String subject, int clientTypeCode, String appCode, String room, String clientId, String consumerGroup, OnOfflineState onlineStatus, ConsumeStrategy consumeStrategy) {
        this.subject = subject;
        this.clientTypeCode = clientTypeCode;
        this.appCode = appCode;
        this.room = room;
        this.clientId = clientId;
        this.consumerGroup = consumerGroup;
        this.onlineStatus = onlineStatus;
        this.consumeStrategy = consumeStrategy;
    }

    public String getSubject() {
        return subject;
    }

    public int getClientTypeCode() {
        return clientTypeCode;
    }

    public String getAppCode() {
        return appCode;
    }

    public String getRoom() {
        return room;
    }

    public String getClientId() {
        return clientId;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public OnOfflineState getOnlineStatus() {
        return onlineStatus;
    }

    public ConsumeStrategy getConsumeStrategy() {
        return consumeStrategy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClientMetaInfo that = (ClientMetaInfo) o;
        return clientTypeCode == that.clientTypeCode &&
                Objects.equals(subject, that.subject) &&
                Objects.equals(appCode, that.appCode) &&
                Objects.equals(room, that.room) &&
                Objects.equals(clientId, that.clientId) &&
                Objects.equals(consumerGroup, that.consumerGroup) &&
                onlineStatus == that.onlineStatus &&
                consumeStrategy == that.consumeStrategy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(subject, clientTypeCode, appCode, room, clientId, consumerGroup, onlineStatus, consumeStrategy);
    }
}
