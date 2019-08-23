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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.backup.base.RecordQuery;
import qunar.tc.qmq.backup.base.RecordQueryResult;
import qunar.tc.qmq.backup.service.MessageService;
import qunar.tc.qmq.backup.util.GsonUtils;

import javax.servlet.AsyncContext;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019-03-05 15:45
 */
public class MessageRecordsServlet extends HttpServlet {
    private static final Logger LOG = LoggerFactory.getLogger(MessageRecordsServlet.class);

    private static final RecordQueryResult EMPTY_RECORD_QUERY_RESULT = new RecordQueryResult(Collections.emptyList());

    private final MessageService messageService;

    public MessageRecordsServlet(MessageService messageService) {
        this.messageService = messageService;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        resp.setStatus(HttpServletResponse.SC_OK);
        RecordQuery query = deserialize(req);
        if (query == null) {
            response(resp);
            return;
        }

        final AsyncContext context = req.startAsync();
        CompletableFuture<RecordQueryResult> future = messageService.findRecords(query);
        future.exceptionally(throwable -> EMPTY_RECORD_QUERY_RESULT).thenAccept(result -> {
            response(resp, GsonUtils.serialize(result));
            context.complete();
        });
    }

    private void response(ServletResponse resp) throws IOException {
        resp.setContentType("application/json");
        RecordQueryResult result = new RecordQueryResult(Collections.emptyList());
        resp.getWriter().println(GsonUtils.serialize(result));
    }

    private void response(ServletResponse resp, Object data) {
        try {
            resp.getWriter().println(data);
        } catch (IOException e) {
            LOG.error("An IOException occurred.", e);
        }
    }

    private RecordQuery deserialize(HttpServletRequest req) {
        try {
            final String recordQueryStr = req.getParameter("recordQuery");
            return GsonUtils.deSerialize(recordQueryStr, RecordQuery.class);
        } catch (Exception e) {
            LOG.error("Get record query failed.", e);
            return null;
        }
    }
}
