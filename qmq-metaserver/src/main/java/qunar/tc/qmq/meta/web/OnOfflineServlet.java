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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.OnOfflineState;
import qunar.tc.qmq.common.JsonHolder;
import qunar.tc.qmq.meta.cache.CachedOfflineStateManager;
import qunar.tc.qmq.meta.model.ClientOfflineState;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * yiqun.fan@qunar.com 2018/3/7
 */
public class OnOfflineServlet extends HttpServlet {
    private static final Logger LOGGER = LoggerFactory.getLogger(OnOfflineServlet.class);

    private static final ObjectMapper MAPPER = JsonHolder.getMapper();

    private final CachedOfflineStateManager offlineStateManager = CachedOfflineStateManager.SUPPLIER.get();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        final String operation = req.getParameter("operation");
        final String clientId = req.getParameter("clientId");
        final String subject = req.getParameter("subject");
        final String consumerGroup = req.getParameter("consumerGroup");
        final OnOfflineState state = getOnOfflineState(req.getParameter("state"));

        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setHeader("Content-Type", "application/json");
        PrintWriter out = resp.getWriter();

        if (Strings.isNullOrEmpty(operation) || Strings.isNullOrEmpty(clientId) || Strings.isNullOrEmpty(subject) || Strings.isNullOrEmpty(consumerGroup) || state == null) {
            out.println(MAPPER.writeValueAsString(new JsonResult<>(ResultStatus.SYSTEM_ERROR, "invalid parameter", null)));
            return;
        }

        ClientOfflineState clientState = new ClientOfflineState();
        clientState.setClientId(clientId);
        clientState.setSubject(subject);
        clientState.setConsumerGroup(consumerGroup);
        clientState.setState(state);

        try {
            if ("update".equals(operation)) {
                offlineStateManager.insertOrUpdate(clientState);
                out.println(MAPPER.writeValueAsString(new JsonResult<Boolean>(ResultStatus.OK, "ok", null)));
            } else if ("query".equals(operation)) {
                offlineStateManager.queryClientState(clientState);
                out.println(MAPPER.writeValueAsString(new JsonResult<Boolean>(ResultStatus.OK, clientState.getState().toString(), null)));
            } else {
                out.println(MAPPER.writeValueAsString(new JsonResult<Boolean>(ResultStatus.SYSTEM_ERROR, "unsupport: " + operation, null)));
            }
        } catch (Exception e) {
            LOGGER.error("onoffline exception. {}", clientState, e);
            out.println(MAPPER.writeValueAsString(new JsonResult<Boolean>(ResultStatus.SYSTEM_ERROR, e.getClass().getCanonicalName() + ": " + e.getMessage(), null)));
        }
    }

    private OnOfflineState getOnOfflineState(String state) {
        try {
            return OnOfflineState.fromCode(Integer.parseInt(state));
        } catch (Exception e) {
            return null;
        }
    }
}
