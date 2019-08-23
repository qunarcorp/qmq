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
import qunar.tc.qmq.common.JsonHolder;
import qunar.tc.qmq.meta.model.GroupedConsumer;
import qunar.tc.qmq.meta.route.SubjectConsumerService;
import qunar.tc.qmq.meta.route.impl.SubjectConsumerServiceImpl;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;

/**
 * @author keli.wang
 * @since 2017/12/5
 */
public class SubjectConsumerServlet extends HttpServlet {
    private static final Logger LOG = LoggerFactory.getLogger(SubjectConsumerServlet.class);

    private static final ObjectMapper MAPPER = JsonHolder.getMapper();

    private final SubjectConsumerService subjectConsumerService = new SubjectConsumerServiceImpl();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        final String subject = req.getParameter("subject");

        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setHeader("Content-Type", "application/json");

        if (Strings.isNullOrEmpty(subject)) {
            resp.getWriter().println(MAPPER.writeValueAsString(new JsonResult<>(ResultStatus.SYSTEM_ERROR, "请求的主题不能为空", null)));
            return;
        }

        try {
            final List<GroupedConsumer> consumers = subjectConsumerService.consumers(subject);
            resp.getWriter().println(MAPPER.writeValueAsString(new JsonResult<>(ResultStatus.OK, "成功", consumers)));
            return;
        } catch (Exception e) {
            LOG.error("Failed get subject consumers. subject: {}", subject, e);
        }

        resp.getWriter().println(MAPPER.writeValueAsString(new JsonResult<Boolean>(ResultStatus.SYSTEM_ERROR, "未知错误", null)));
    }
}
