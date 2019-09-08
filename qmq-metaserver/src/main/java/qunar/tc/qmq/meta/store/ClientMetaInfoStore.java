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

package qunar.tc.qmq.meta.store;

import qunar.tc.qmq.base.OnOfflineState;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.meta.model.ClientMetaInfo;

import java.util.Date;
import java.util.List;

/**
 * @author keli.wang
 * @since 2017/12/5
 */
public interface ClientMetaInfoStore {

    List<ClientMetaInfo> queryConsumer(final String subject);

    /**
     * 获取在指定时间之后更新过的 client
     *
     * @param clientType client type
     * @param onlineStatus online status
     * @return client list
     */
    List<ClientMetaInfo> queryClientsUpdateAfterDate(ClientType clientType, OnOfflineState onlineStatus, Date updateDate);

    List<ClientMetaInfo> queryClientsUpdateAfterDate(String subject, String consumerGroup, ClientType clientType, OnOfflineState onlineStatus, Date updateDate);

    int updateClientOnlineState(ClientMetaInfo clientMetaInfo);
}
