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

package qunar.tc.qmq.backup.api;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.backup.base.BackupQuery;
import qunar.tc.qmq.backup.base.MessageQueryResult;
import qunar.tc.qmq.backup.service.MessageService;
import qunar.tc.qmq.backup.util.GsonUtils;

import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collections;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019-03-05 14:47
 */
public abstract class AbstractGetServlet extends HttpServlet {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractGetServlet.class);

    final MessageService messageService;

    protected static final MessageQueryResult EMPTY_MESSAGE_QUERY_RESULT = new MessageQueryResult();

    AbstractGetServlet(MessageService messageService) {
        this.messageService = messageService;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        resp.setStatus(HttpServletResponse.SC_OK);
        final String queryStr = req.getParameter("backupQuery");
        if (Strings.isNullOrEmpty(queryStr)) return;
        final BackupQuery query = deserialize(queryStr);
        if (query == null) {
            response(resp, GsonUtils.serialize(Collections.emptyList()));
            return;
        }

        query(req, resp, query);
    }

    void response(ServletResponse resp, Object data) {
        resp.setContentType("application/json");
        resp.setCharacterEncoding("UTF-8");
        try {
            resp.getWriter().println(data);
        } catch (IOException e) {
            LOG.error("An IOException occurred.", e);
        }
    }

    protected abstract void query(final HttpServletRequest req, final HttpServletResponse resp, final BackupQuery query) throws IOException;

    private BackupQuery deserialize(final String json) {
        try {
            return GsonUtils.deSerialize(json, BackupQuery.class);
        } catch (Exception e) {
            LOG.error("Get backup query error.", e);
            return null;
        }
    }
}
