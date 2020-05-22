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

package qunar.tc.qmq.broker;

import qunar.tc.qmq.common.ClientType;

/**
 * @author yiqun.fan create on 17-8-18.
 */
public interface BrokerService {

    BrokerClusterInfo getClusterBySubject(ClientType clientType, String subject);

    BrokerClusterInfo getClusterBySubject(ClientType clientType, String subject, String group);

    void refresh(ClientType clientType, String subject);

    void refresh(ClientType clientType, String subject, String group);

    void setAppCode(String appCode);
}
