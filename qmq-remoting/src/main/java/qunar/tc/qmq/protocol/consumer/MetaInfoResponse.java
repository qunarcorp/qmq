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

package qunar.tc.qmq.protocol.consumer;

import qunar.tc.qmq.meta.BrokerCluster;
import qunar.tc.qmq.base.OnOfflineState;

/**
 * @author yiqun.fan create on 17-8-31.
 */
public class MetaInfoResponse {
    private long timestamp;
    private String subject;
    private String consumerGroup;
    private OnOfflineState onOfflineState;
    private int clientTypeCode;
    private BrokerCluster brokerCluster;

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public OnOfflineState getOnOfflineState() {
        return onOfflineState;
    }

    public void setOnOfflineState(OnOfflineState onOfflineState) {
        this.onOfflineState = onOfflineState;
    }

    public int getClientTypeCode() {
        return clientTypeCode;
    }

    public void setClientTypeCode(int clientTypeCode) {
        this.clientTypeCode = clientTypeCode;
    }

    public BrokerCluster getBrokerCluster() {
        return brokerCluster;
    }

    public void setBrokerCluster(BrokerCluster brokerCluster) {
        this.brokerCluster = brokerCluster;
    }

    @Override
    public String toString() {
        return "MetaInfoResponse{" +
                "timestamp=" + timestamp +
                ", subject='" + subject + '\'' +
                ", consumerGroup='" + consumerGroup + '\'' +
                ", onOfflineState=" + onOfflineState +
                ", clientTypeCode=" + clientTypeCode +
                ", brokerCluster=" + brokerCluster +
                '}';
    }
}
