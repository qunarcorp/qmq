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
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.meta.cache.CachedMetaInfoManager;
import qunar.tc.qmq.meta.model.SubjectRoute;
import qunar.tc.qmq.meta.store.Store;
import qunar.tc.qmq.meta.utils.SubjectUtils;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Created by zhaohui.yu
 * 7/24/18
 */
public class ExtendSubjectRouteAction implements MetaManagementAction {
    private static final Logger LOG = LoggerFactory.getLogger(ExtendSubjectRouteAction.class);

    private final Store store;
    private final CachedMetaInfoManager cache;

    public ExtendSubjectRouteAction(Store store, CachedMetaInfoManager cache) {
        this.store = store;
        this.cache = cache;
    }

    @Override
    public Object handleAction(HttpServletRequest req) {
        final String relatedSubject = req.getParameter("relatedSubject");
        final String relatedBrokerGroup = req.getParameter("relatedBrokerGroup");
        final String newBrokerGroup = req.getParameter("newBrokerGroup");
        final boolean isAnySubject = SubjectUtils.isAnySubject(relatedSubject);

        if (Strings.isNullOrEmpty(relatedSubject)
                || Strings.isNullOrEmpty(relatedBrokerGroup)
                || Strings.isNullOrEmpty(newBrokerGroup)) {
            return ActionResult.error("必须同时提供 relatedSubject、relatedBrokerGroup 和 newBrokerGroup 三个参数");
        }

        int updated = 0;
        final List<SubjectRoute> routes = store.getAllSubjectRoutes();
        try {
            for (SubjectRoute route : routes) {
                final String subject = route.getSubject();
                if (!isAnySubject && !Objects.equals(subject, relatedSubject)) {
                    continue;
                }

                final List<String> brokerGroups = route.getBrokerGroups();
                if (!brokerGroups.contains(relatedBrokerGroup)) continue;
                if (brokerGroups.contains(newBrokerGroup)) continue;

                final Set<String> newBrokers = Sets.newHashSet(brokerGroups);
                newBrokers.add(newBrokerGroup);

                final int affectedRows = store.updateSubjectRoute(subject, route.getVersion(), new ArrayList<>(newBrokers));
                if (affectedRows == 1) {
                    LOG.info("update subject route: {} from {} to {}", subject, brokerGroups, newBrokers);
                    updated++;
                }
            }
        } finally {
            cache.executeRefreshTask();
        }

        return ActionResult.ok("成功更新" + updated + "个subject的路由");
    }
}
