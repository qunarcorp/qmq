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

package qunar.tc.qmq.meta.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import qunar.tc.qmq.common.JsonHolder;
import qunar.tc.qmq.meta.management.MetaManagementAction;
import qunar.tc.qmq.meta.management.MetaManagementActionSupplier;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @author keli.wang
 * @since 2017/10/20
 */
public class MetaManagementServlet extends HttpServlet {
    private static final ObjectMapper MAPPER = JsonHolder.getMapper();

    private final MetaManagementActionSupplier actions = MetaManagementActionSupplier.getInstance();

    @Override
    protected void doPost(final HttpServletRequest req, final HttpServletResponse resp) throws IOException {
        final String actionName = req.getParameter("action");
        if (Strings.isNullOrEmpty(actionName)) {
            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().println("need provide action param");
            return;
        }

        final MetaManagementAction action = actions.getAction(actionName);
        if (action == null) {
            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().println("不支持的 action: " + actionName);
            return;
        }

        final Object result = action.handleAction(req);
        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setHeader("Content-Type", "application/json");
        resp.getWriter().println(MAPPER.writeValueAsString(result));
    }
}
