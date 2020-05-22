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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.meta.BrokerRole;
import qunar.tc.qmq.meta.model.BrokerMeta;
import qunar.tc.qmq.meta.store.BrokerStore;

import javax.servlet.http.HttpServletRequest;
import java.util.Optional;

/**
 * @author keli.wang
 * @since 2018-12-03
 */
public class ReplaceBrokerAction implements MetaManagementAction {
    private static final Logger LOG = LoggerFactory.getLogger(ReplaceBrokerAction.class);

    private static final int RETRY_REPLACE_COUNT = 20;

    private final BrokerStore store;

    public ReplaceBrokerAction(final BrokerStore store) {
        this.store = store;
    }

    @Override
    public ActionResult<BrokerMeta> handleAction(final HttpServletRequest req) {
        try {
            final String brokerGroup = req.getParameter("brokerGroup");
            final BrokerRole role = BrokerRole.fromCode(Integer.parseInt(req.getParameter("role")));
            if (Strings.isNullOrEmpty(brokerGroup)) {
                return ActionResult.error("should provide broker group name");
            }

            final String hostname = req.getParameter("hostname");
            final String ip = req.getParameter("ip");
            final int servePort = Integer.parseInt(req.getParameter("servePort"));
            final int syncPort = Integer.parseInt(req.getParameter("syncPort"));
            final BrokerMeta broker = new BrokerMeta(brokerGroup, role, hostname, ip, servePort, syncPort);

            final Optional<String> validateResult = validateBroker(broker);
            if (validateResult.isPresent()) {
                return ActionResult.error(validateResult.get());
            }

            for (int i = 0; i < RETRY_REPLACE_COUNT; i++) {
                final Optional<BrokerMeta> current = store.queryByRole(brokerGroup, role.getCode());
                if (!current.isPresent()) {
                    return ActionResult.error("no exist broker with this broker group name and role");
                } else {
                    final int result = store.replaceBrokerByRole(current.get(), broker);
                    if (result > 0) {
                        return ActionResult.ok(current.get());
                    }
                }
            }

            return ActionResult.error("replace broker failed. retry count: " + RETRY_REPLACE_COUNT);
        } catch (Exception e) {
            LOG.error("replace broker failed.", e);
            return ActionResult.error("replace broker action failed, caused by: " + e.getMessage());
        }
    }

    private Optional<String> validateBroker(final BrokerMeta broker) {
        if (Strings.isNullOrEmpty(broker.getHostname())) {
            return Optional.of("please provide broker hostname");
        }
        if (Strings.isNullOrEmpty(broker.getIp())) {
            return Optional.of("please provide broker ip");
        }

        final int servePort = broker.getServePort();
        final int syncPort = broker.getSyncPort();
        if (servePort <= 0 || syncPort <= 0 || servePort == syncPort) {
            return Optional.of("serve port and sync port should valid and should be different port");
        }

        return Optional.empty();
    }
}
