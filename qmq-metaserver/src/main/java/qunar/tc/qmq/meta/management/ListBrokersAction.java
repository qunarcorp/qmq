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
import qunar.tc.qmq.meta.model.BrokerMeta;
import qunar.tc.qmq.meta.store.BrokerStore;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

/**
 * @author keli.wang
 * @since 2018-12-03
 */
public class ListBrokersAction implements MetaManagementAction {
    private static final Logger LOG = LoggerFactory.getLogger(ListBrokersAction.class);

    private final BrokerStore store;

    public ListBrokersAction(final BrokerStore store) {
        this.store = store;
    }

    @Override
    public ActionResult<List<BrokerMeta>> handleAction(final HttpServletRequest req) {
        final String brokerGroup = req.getParameter("brokerGroup");
        try {
            if (Strings.isNullOrEmpty(brokerGroup)) {
                return ActionResult.ok(store.allBrokers());
            } else {
                return ActionResult.ok(store.queryBrokers(brokerGroup));
            }
        } catch (Exception e) {
            LOG.error("list brokers failed.", e);
            return ActionResult.error("list brokers action failed");
        }
    }
}
