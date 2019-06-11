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
import qunar.tc.qmq.meta.model.ReadonlyBrokerGroupSetting;
import qunar.tc.qmq.meta.service.ReadonlyBrokerGroupSettingService;

import javax.servlet.http.HttpServletRequest;

/**
 * @author keli.wang
 * @since 2018/7/30
 */
public class UnMarkReadonlyBrokerGroupAction implements MetaManagementAction {
    private static final Logger LOG = LoggerFactory.getLogger(UnMarkReadonlyBrokerGroupAction.class);

    private final ReadonlyBrokerGroupSettingService service;

    public UnMarkReadonlyBrokerGroupAction(final ReadonlyBrokerGroupSettingService service) {
        this.service = service;
    }

    @Override
    public Object handleAction(final HttpServletRequest req) {
        final String subject = req.getParameter("subject");
        final String brokerGroup = req.getParameter("brokerGroup");

        if (Strings.isNullOrEmpty(subject) || Strings.isNullOrEmpty(brokerGroup)) {
            return ActionResult.error("必须同时提供 subject 和 brokerGroup 两个参数");
        }

        try {
            service.removeSetting(new ReadonlyBrokerGroupSetting(subject, brokerGroup));
            return ActionResult.ok("success");
        } catch (Exception e) {
            LOG.error("remove readonly broker group setting failed. subject: {}, brokerGroup: {}", subject, brokerGroup, e);
            return ActionResult.error(e.getMessage());
        }
    }
}
