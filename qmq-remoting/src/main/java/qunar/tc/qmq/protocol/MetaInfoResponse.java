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

package qunar.tc.qmq.protocol;

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

    public MetaInfoResponse(long timestamp, String subject, String consumerGroup, OnOfflineState onOfflineState, int clientTypeCode, BrokerCluster brokerCluster) {
        this.timestamp = timestamp;
        this.subject = subject;
        this.consumerGroup = consumerGroup;
        this.onOfflineState = onOfflineState;
        this.clientTypeCode = clientTypeCode;
        this.brokerCluster = brokerCluster;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getSubject() {
        return subject;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public OnOfflineState getOnOfflineState() {
        return onOfflineState;
    }

    public int getClientTypeCode() {
        return clientTypeCode;
    }

    public BrokerCluster getBrokerCluster() {
        return brokerCluster;
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
