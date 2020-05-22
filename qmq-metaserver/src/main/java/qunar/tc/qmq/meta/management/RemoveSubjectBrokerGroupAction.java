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
import org.springframework.dao.EmptyResultDataAccessException;
import qunar.tc.qmq.meta.cache.CachedMetaInfoManager;
import qunar.tc.qmq.meta.model.SubjectRoute;
import qunar.tc.qmq.meta.store.Store;
import qunar.tc.qmq.meta.utils.SubjectUtils;
import qunar.tc.qmq.meta.web.ResultStatus;

import javax.servlet.http.HttpServletRequest;
import java.util.*;

/**
 * @author keli.wang
 * @since 2017/10/20
 */
public class RemoveSubjectBrokerGroupAction implements MetaManagementAction {
    private final Store store;
    private final CachedMetaInfoManager cacheManager;

    public RemoveSubjectBrokerGroupAction(final Store store, final CachedMetaInfoManager cacheManager) {
        this.store = store;
        this.cacheManager = cacheManager;
    }

    @Override
    public Object handleAction(final HttpServletRequest req) {
        final String brokerGroup = req.getParameter("brokerGroup");
        final String subject = req.getParameter("subject");

        if (Strings.isNullOrEmpty(brokerGroup) || Strings.isNullOrEmpty(subject)) {
            return ActionResult.error("必须同时提供 brokerGroup 和 subject 两个参数");
        }

        if (SubjectUtils.isAnySubject(subject)) {
            return removeBrokerGroupForAnySubject(brokerGroup);
        } else {
            return removeBrokerGroupForOneSubject(subject, brokerGroup);
        }
    }

    private ActionResult<Map<String, String>> removeBrokerGroupForAnySubject(final String brokerGroup) {
        final Map<String, String> errors = new HashMap<>();

        final List<SubjectRoute> routes = store.getAllSubjectRoutes();
        try {
            for (final SubjectRoute route : routes) {
                final List<String> brokerGroups = route.getBrokerGroups();
                if (!brokerGroups.contains(brokerGroup)) {
                    continue;
                }

                final ActionResult<String> result = removeBrokerGroupFromRoute(brokerGroup, route);
                if (result.getStatus() != ResultStatus.OK) {
                    errors.put(route.getSubject(), result.getMessage());
                }
            }
        } finally {
            cacheManager.executeRefreshTask();
        }

        if (errors.isEmpty()) {
            return ActionResult.ok(Collections.emptyMap());
        } else {
            return ActionResult.error("部分主题路由缩减失败", errors);
        }
    }

    private ActionResult<Map<String, String>> removeBrokerGroupForOneSubject(final String subject, final String brokerGroup) {
        final SubjectRoute oldRoute = querySubjectRoute(subject);
        if (oldRoute == null) {
            return ActionResult.error("提供的 subject 暂时没有分配过 brokerGroup", Collections.emptyMap());
        }

        try {
            final ActionResult<String> result = removeBrokerGroupFromRoute(brokerGroup, oldRoute);
            if (result.getStatus() == ResultStatus.OK) {
                return ActionResult.ok(Collections.emptyMap());
            } else {
                return ActionResult.error("路由缩减失败", Collections.singletonMap(subject, result.getMessage()));
            }
        } finally {
            cacheManager.executeRefreshTask();
        }
    }

    private SubjectRoute querySubjectRoute(final String subject) {
        try {
            return store.selectSubjectRoute(subject);
        } catch (EmptyResultDataAccessException ignore) {
            return null;
        }
    }

    private ActionResult<String> removeBrokerGroupFromRoute(final String brokerGroup, final SubjectRoute route) {
        final List<String> brokerGroups = new ArrayList<>(route.getBrokerGroups());
        if (!brokerGroups.contains(brokerGroup)) {
            return ActionResult.ok("ok");
        }

        brokerGroups.remove(brokerGroup);
        if (brokerGroups.isEmpty()) {
            return ActionResult.error("不允许删除主题的最后一个 brokerGroup");
        }

        final int affectedRows = store.updateSubjectRoute(route.getSubject(), route.getVersion(), brokerGroups);
        if (affectedRows == 1) {
            return ActionResult.ok("ok");
        } else {
            return ActionResult.error("缩减 subject 对应 brokerGroup 失败");
        }
    }
}
