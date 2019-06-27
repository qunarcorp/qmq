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

package qunar.tc.qmq.meta.management;

import com.google.common.base.Strings;
import qunar.tc.qmq.meta.BrokerRole;
import qunar.tc.qmq.meta.model.BrokerMeta;
import qunar.tc.qmq.meta.store.BrokerStore;

import javax.servlet.http.HttpServletRequest;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

/**
 * @author keli.wang
 * @since 2018-12-03
 */
public class AddBrokerAction implements MetaManagementAction {

    private static final HashSet<BrokerRole> MATCHED_ROLES = new HashSet<>();

    static {
        MATCHED_ROLES.add(BrokerRole.MASTER);
        MATCHED_ROLES.add(BrokerRole.SLAVE);
        MATCHED_ROLES.add(BrokerRole.DELAY_MASTER);
        MATCHED_ROLES.add(BrokerRole.DELAY_SLAVE);
        MATCHED_ROLES.add(BrokerRole.BACKUP);
        MATCHED_ROLES.add(BrokerRole.DELAY_BACKUP);
    }

    private final BrokerStore store;

    public AddBrokerAction(final BrokerStore store) {
        this.store = store;
    }

    @Override
    public ActionResult<BrokerMeta> handleAction(final HttpServletRequest req) {
        try {
            final String brokerGroup = req.getParameter("brokerGroup");
            final BrokerRole role = BrokerRole.fromCode(Integer.parseInt(req.getParameter("role")));
            final String hostname = req.getParameter("hostname");
            final String ip = req.getParameter("ip");
            final int servePort = Integer.parseInt(req.getParameter("servePort"));
            int syncPort = -1;
            if (notBackup(role)) {
                syncPort = Integer.parseInt(req.getParameter("syncPort"));
            }
            final BrokerMeta broker = new BrokerMeta(brokerGroup, role, hostname, ip, servePort, syncPort);

            final Optional<String> validateResult = validateBroker(broker);
            if (validateResult.isPresent()) {
                return ActionResult.error(validateResult.get());
            }

            final int result = store.insertBroker(broker);
            if (result > 0) {
                return ActionResult.ok(broker);
            } else {
                return ActionResult.error("broker group's role already exist or broker already added");
            }
        } catch (Exception e) {
            return ActionResult.error("add broker failed, caused by: " + e.getMessage());
        }
    }

    private Optional<String> validateBroker(final BrokerMeta broker) {
        if (Strings.isNullOrEmpty(broker.getGroup())) {
            return Optional.of("please provide broker group name");
        }
        if (!MATCHED_ROLES.contains(broker.getRole())) {
            return Optional.of("invalid broker role code " + broker.getRole().getCode());
        }
        if (Strings.isNullOrEmpty(broker.getHostname())) {
            return Optional.of("please provide broker hostname");
        }
        if (Strings.isNullOrEmpty(broker.getIp())) {
            return Optional.of("please provide broker ip");
        }

        final int servePort = broker.getServePort();
        final int syncPort = broker.getSyncPort();
        if (servePort <= 0 || (syncPort <= 0 && notBackup(broker.getRole())) || servePort == syncPort) {
            return Optional.of("serve port and sync port should valid and should be different port");
        }

        List<BrokerMeta> brokers = store.queryBrokers(broker.getGroup());
        if (brokers == null || brokers.isEmpty()) return Optional.empty();

        if (brokers.size() >= 3) {
            return Optional.of("The brokerGroup: " + broker.getGroup() + " already exists");
        }

        if (exist(brokers, broker)) {
            return Optional.of("The brokerGroup: " + broker.getGroup() + " with role: " + broker.getRole() + " already exists, you need use other brokerGroup name");
        }

        return Optional.empty();
    }

    private boolean notBackup(BrokerRole role) {
        return role != BrokerRole.BACKUP && role != BrokerRole.DELAY_BACKUP;
    }

    private boolean exist(final List<BrokerMeta> brokers, final BrokerMeta broker) {
        for (BrokerMeta brokerMeta : brokers) {
            if (broker.getRole() == brokerMeta.getRole()) return true;
        }

        return false;
    }
}
