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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.springframework.dao.EmptyResultDataAccessException;
import qunar.tc.qmq.meta.cache.CachedMetaInfoManager;
import qunar.tc.qmq.meta.model.SubjectRoute;
import qunar.tc.qmq.meta.store.Store;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.Set;

/**
 * @author keli.wang
 * @since 2017/10/20
 */
public class AddSubjectBrokerGroupAction implements MetaManagementAction {
    private final Store store;
    private final CachedMetaInfoManager cacheManager;

    public AddSubjectBrokerGroupAction(final Store store, final CachedMetaInfoManager cacheManager) {
        this.store = store;
        this.cacheManager = cacheManager;
    }

    @Override
    public Object handleAction(final HttpServletRequest req) {
        final String brokerGroup = req.getParameter("brokerGroup");
        final String subject = req.getParameter("subject");

        if (Strings.isNullOrEmpty(brokerGroup) || Strings.isNullOrEmpty(subject)) {
            return ImmutableMap.of(
                    "error", "必须同时提供 brokerGroup 和 subject 两个参数"
            );
        }

        if (!isBrokerGroupExist(brokerGroup)) {
            return ImmutableMap.of(
                    "error", "提供的 brokerGroup 暂时不存在"
            );
        }

        final SubjectRoute oldRoute = querySubjectRoute(subject);
        if (oldRoute == null) {
            return ImmutableMap.of(
                    "error", "提供的 subject 暂时没有分配过 brokerGroup，无法扩充"
            );
        }

        final Set<String> brokerGroups = Sets.newHashSet(oldRoute.getBrokerGroups());
        brokerGroups.add(brokerGroup);
        final int affectedRows = store.updateSubjectRoute(subject, oldRoute.getVersion(), new ArrayList<>(brokerGroups));
        cacheManager.executeRefreshTask();

        if (affectedRows == 1) {
            return store.selectSubjectRoute(subject);
        } else {
            return ImmutableMap.of(
                    "error", "扩充 subject 对应 brokerGroup 失败，请参考下面的当前配置",
                    "subjectRoute", store.selectSubjectRoute(subject)
            );
        }
    }

    private SubjectRoute querySubjectRoute(final String subject) {
        try {
            return store.selectSubjectRoute(subject);
        } catch (EmptyResultDataAccessException ignore) {
            return null;
        }
    }

    private boolean isBrokerGroupExist(final String brokerGroup) {
        return store.getBrokerGroup(brokerGroup) != null;
    }
}
