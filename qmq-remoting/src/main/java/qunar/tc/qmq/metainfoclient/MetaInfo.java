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

package qunar.tc.qmq.metainfoclient;

import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.ClientType;

/**
 * @author yiqun.fan create on 17-8-31.
 */
public class MetaInfo {
    private final String subject;
    private final ClientType clientType;
    private final BrokerClusterInfo clusterInfo;

    public MetaInfo(String subject, ClientType clientType, BrokerClusterInfo clusterInfo) {
        this.subject = subject;
        this.clientType = clientType;
        this.clusterInfo = clusterInfo;
    }

    public String getSubject() {
        return subject;
    }

    public ClientType getClientType() {
        return clientType;
    }

    public BrokerClusterInfo getClusterInfo() {
        return clusterInfo;
    }

    @Override
    public String toString() {
        return "MetaInfo{" +
                "subject='" + subject + '\'' +
                ", clientType=" + clientType +
                ", groups=" + clusterInfo.getGroups() +
                '}';
    }
}
