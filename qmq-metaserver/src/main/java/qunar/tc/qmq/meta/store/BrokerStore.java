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

import qunar.tc.qmq.meta.model.BrokerMeta;

import java.util.List;
import java.util.Optional;

/**
 * @author keli.wang
 * @since 2018-11-29
 */
public interface BrokerStore {
    Optional<BrokerMeta> queryBroker(final String hostname, final int servePort);

    Optional<BrokerMeta> queryByRole(final String brokerGroup, final int role);

    List<BrokerMeta> queryBrokers(final String groupName);

    List<BrokerMeta> allBrokers();

    int insertBroker(final BrokerMeta broker);

    int replaceBrokerByRole(final BrokerMeta oldBroker, final BrokerMeta newBroker);
}
