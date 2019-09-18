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

package qunar.tc.qmq.gateway.servlet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.PullConsumer;
import qunar.tc.qmq.common.JsonHolder;
import qunar.tc.qmq.consumer.MessageConsumerProvider;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.metrics.MetricsConstants;

import javax.servlet.*;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhaohui.yu
 * 9/29/17
 */
public class PullServlet extends HttpServlet {
    private static final Logger logger = LoggerFactory.getLogger(PullServlet.class);

    private MessageConsumerProvider consumer;

    private final ObjectMapper MAPPER = JsonHolder.getMapper();

    private Executor writeExecutor;

    private static final int DEFAULT_NETWORK_TTL = 2000;

    @Override
    public void init() throws ServletException {
        consumer = new MessageConsumerProvider();
        consumer.setMetaServer(null); // TODO(zhenwei.liu) 这个 meta server 地址从哪儿来
        consumer.init();
        writeExecutor = Executors.newCachedThreadPool();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        req.setCharacterEncoding("UTF-8");
        String subject = extractSubject(req);
        String group = req.getParameter("group");
        if (inValid(subject, group)) {
            Map<String, Object> result = new HashMap<>();
            result.put("state", "-2");
            result.put("data", "invalid parameter");
            write(result, resp);
            return;
        }

        long timeout = Long.parseLong(req.getParameter("timeout"));
        int batch = Integer.parseInt(req.getParameter("batch"));

        long start = System.currentTimeMillis();
        PullConsumer pullConsumer = consumer.getOrCreatePullConsumer(subject, group, false);
        pullConsumer.setConsumeMostOnce(true);
        final AsyncContext asyncContext = req.startAsync();
        asyncContext.addListener(new PullAsyncListener());
        if (timeout < 0) {
            asyncContext.setTimeout(DEFAULT_NETWORK_TTL);
        } else {
            asyncContext.setTimeout(timeout + DEFAULT_NETWORK_TTL);
        }
        ListenableFuture<List<Message>> future = (ListenableFuture<List<Message>>) pullConsumer.pullFuture(batch, timeout);
        future.addListener(() -> {
            try {
                try {
                    List<Message> messages = future.get();
                    Metrics.meter("pullMessages", MetricsConstants.SUBJECT_GROUP_ARRAY, new String[]{subject, group}).mark(messages.size());
                    if (messages.size() > 0) {
                        Map<String, Object> result = new HashMap<>();
                        result.put("state", "0");
                        result.put("data", messages);
                        write(subject, group, result, asyncContext);
                    } else {
                        Map<String, Object> result = new HashMap<>();
                        result.put("state", "-2");
                        result.put("error", "no message");
                        write(subject, group, result, asyncContext);
                    }
                } catch (Exception e) {
                    Metrics.counter("pullError", MetricsConstants.SUBJECT_GROUP_ARRAY, new String[]{subject, group}).inc();
                    Map<String, Object> result = new HashMap<>();
                    result.put("state", "-1");
                    result.put("error", e.getMessage());
                    logger.error("error", e);
                    write(subject, group, result, asyncContext);
                } finally {
                    Metrics.timer("pullMessageTime", MetricsConstants.SUBJECT_GROUP_ARRAY, new String[]{subject, group}).update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
                    asyncContext.complete();
                }
            } catch (Exception ex) {
                logger.error("error", ex);
            }
        }, writeExecutor);
    }

    private boolean inValid(String subject, String group) {
        return subject == null || subject.length() <= 0
                || group == null || group.length() <= 0;
    }

    private String extractSubject(HttpServletRequest req) {
        String pathInfo = req.getPathInfo();
        if (pathInfo == null || pathInfo.length() <= 0) return null;

        if (pathInfo.startsWith("/")) {
            return pathInfo.substring(1);
        }
        return pathInfo;
    }

    private void write(String subject, String group, Object result, AsyncContext asyncContext) {
        ServletResponse response = asyncContext.getResponse();
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        try {
            String content = MAPPER.writeValueAsString(result);
            response.getWriter().write(content);
        } catch (Exception e) {
            logger.error("write message out failed {}-{}", subject, group, e);
        }
    }

    private void write(Object result, ServletResponse response) {
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        try {
            String content = MAPPER.writeValueAsString(result);
            response.getWriter().write(content);
        } catch (IOException e) {
            logger.error("write client failed");
        }
    }

    class PullAsyncListener implements AsyncListener {

        @Override
        public void onComplete(AsyncEvent asyncEvent) throws IOException {

        }

        @Override
        public void onTimeout(AsyncEvent asyncEvent) throws IOException {
            Map<String, Object> result = new HashMap<>();
            result.put("state", "-3");
            result.put("data", "time out");
            ServletResponse response = asyncEvent.getAsyncContext().getResponse();
            response.setContentType("application/json");
            response.setCharacterEncoding("UTF-8");
            response.getWriter().write(MAPPER.writeValueAsString(result));
        }

        @Override
        public void onError(AsyncEvent asyncEvent) throws IOException {
        }

        @Override
        public void onStartAsync(AsyncEvent asyncEvent) throws IOException {
        }
    }

}
