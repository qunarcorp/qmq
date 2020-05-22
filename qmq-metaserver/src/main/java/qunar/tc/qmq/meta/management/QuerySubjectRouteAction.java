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
import qunar.tc.qmq.meta.model.SubjectRoute;
import qunar.tc.qmq.meta.store.Store;
import qunar.tc.qmq.meta.utils.SubjectUtils;

import javax.servlet.http.HttpServletRequest;

/**
 * @author keli.wang
 * @since 2018/8/15
 */
public class QuerySubjectRouteAction implements MetaManagementAction {
    private static final Logger LOG = LoggerFactory.getLogger(QuerySubjectRouteAction.class);

    private final Store store;

    public QuerySubjectRouteAction(final Store store) {
        this.store = store;
    }

    @Override
    public Object handleAction(final HttpServletRequest req) {
        final String subject = req.getParameter("subject");
        if (Strings.isNullOrEmpty(subject)) {
            return ActionResult.error("subject不能为空");
        }
        if (SubjectUtils.isAnySubject(subject)) {
            return ActionResult.error("subject不能为*通配符");
        }

        try {
            final SubjectRoute route = store.selectSubjectRoute(subject);
            return ActionResult.ok(route);
        } catch (Exception e) {
            LOG.error("query subject route failed. subject: {}", subject, e);
            return ActionResult.error(e.getMessage());
        }
    }
}
