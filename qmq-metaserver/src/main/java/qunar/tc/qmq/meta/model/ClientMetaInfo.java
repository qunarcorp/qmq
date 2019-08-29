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

import qunar.tc.qmq.base.OnOfflineState;

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

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public int getClientTypeCode() {
        return clientTypeCode;
    }

    public void setClientTypeCode(int clientTypeCode) {
        this.clientTypeCode = clientTypeCode;
    }

    public String getAppCode() {
        return appCode;
    }

    public void setAppCode(String appCode) {
        this.appCode = appCode;
    }

    public String getRoom() {
        return room;
    }

    public void setRoom(String room) {
        this.room = room;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public OnOfflineState getOnlineStatus() {
        return onlineStatus;
    }

    public ClientMetaInfo setOnlineStatus(OnOfflineState onlineStatus) {
        this.onlineStatus = onlineStatus;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ClientMetaInfo)) return false;

        ClientMetaInfo that = (ClientMetaInfo) o;

        if (clientTypeCode != that.clientTypeCode) return false;
        if (subject != null ? !subject.equals(that.subject) : that.subject != null) return false;
        if (appCode != null ? !appCode.equals(that.appCode) : that.appCode != null) return false;
        if (clientId != null ? !clientId.equals(that.clientId) : that.clientId != null) return false;
        return consumerGroup != null ? consumerGroup.equals(that.consumerGroup) : that.consumerGroup == null;
    }

    @Override
    public int hashCode() {
        int result = subject != null ? subject.hashCode() : 0;
        result = 31 * result + clientTypeCode;
        result = 31 * result + (appCode != null ? appCode.hashCode() : 0);
        result = 31 * result + (clientId != null ? clientId.hashCode() : 0);
        result = 31 * result + (consumerGroup != null ? consumerGroup.hashCode() : 0);
        return result;
    }
}
