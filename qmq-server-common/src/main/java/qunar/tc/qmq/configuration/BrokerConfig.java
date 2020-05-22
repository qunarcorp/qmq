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

package qunar.tc.qmq.configuration;

import com.google.common.eventbus.Subscribe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.meta.BrokerAcquireMetaResponse;
import qunar.tc.qmq.meta.BrokerRole;
import qunar.tc.qmq.utils.NetworkUtils;

/**
 * @author yunfeng.yang
 * @since 2017/8/19
 */
public final class BrokerConfig {
    private static final Logger LOG = LoggerFactory.getLogger(BrokerConfig.class);

    private static final BrokerConfig CONFIG = new BrokerConfig();

    private volatile String brokerName;
    private volatile BrokerRole brokerRole;
    private volatile String brokerAddress;
    private volatile String masterAddress;
    private volatile boolean readonly;

    private BrokerConfig() {
        brokerName = "";
        brokerRole = BrokerRole.STANDBY;
        brokerAddress = NetworkUtils.getLocalAddress();
        masterAddress = "";
        readonly = true;
    }

    public static BrokerConfig getInstance() {
        return CONFIG;
    }

    public static String getBrokerName() {
        return CONFIG.brokerName;
    }

    public static BrokerRole getBrokerRole() {
        return CONFIG.brokerRole;
    }

    public static String getBrokerAddress() {
        return CONFIG.brokerAddress;
    }

    public static String getMasterAddress() {
        return CONFIG.masterAddress;
    }

    public static boolean isReadonly() {
        return CONFIG.readonly;
    }

    public static void markAsWritable() {
        CONFIG.readonly = false;
    }

    @Subscribe
    public void updateMeta(final BrokerAcquireMetaResponse response) {
        LOG.info("Broker meta updated. meta: {}", response);
        if (response.getRole() != BrokerRole.STANDBY) {
            brokerRole = response.getRole();
            brokerName = response.getName();
            masterAddress = response.getMaster();
        }
    }
}
