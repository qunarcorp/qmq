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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.MessageSendStateListener;
import qunar.tc.qmq.common.JsonHolder;
import qunar.tc.qmq.producer.MessageProducerProvider;

import javax.servlet.AsyncContext;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhaohui.yu
 * 1/16/18
 */
public class SendServlet extends HttpServlet {
    private static final Logger logger = LoggerFactory.getLogger(SendServlet.class);

    private MessageProducerProvider producer;

    private final ObjectMapper MAPPER = JsonHolder.getMapper();

    @Override
    public void init(ServletConfig config) throws ServletException {
        this.producer = new MessageProducerProvider(null, null); // TODO(zhenwei.liu) 这里需要写入 appCode, meta server 地址
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        req.setCharacterEncoding("UTF-8");
        String subject = req.getPathInfo();
        if (subject == null || subject.length() == 0) {
            error(resp, "subject is required");
            return;
        }

        InputStream is = req.getInputStream();
        HashMap<String, Object> map = MAPPER.readValue(is, new TypeReference<HashMap<String, Object>>() {
        });

        Object o = map.get("appCode");
        if (o == null) {
            error(resp, "appCode is required");
            return;
        }

        String appCode = o.toString();
        if (appCode.length() == 0) {
            error(resp, "appCode is empty");
            return;
        }

        AsyncContext asyncContext = req.startAsync();
        sendMessage(subject, appCode, map, asyncContext);

    }

    private void sendMessage(String subject, String appid, Map<String, Object> data, AsyncContext asyncContext) {
        Message message = producer.generateMessage(subject);
        message.setProperty("client_app", appid);
        appendAttrs(data, message);
        producer.sendMessage(message, new MessageSendStateListener() {
            @Override
            public void onSuccess(Message message) {
                asyncSuccess(asyncContext, message);
            }

            @Override
            public void onFailed(Message message) {
                asyncError(asyncContext, message);
            }
        });
    }

    private void appendAttrs(Map<String, Object> data, Message message) {
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            if (entry.getKey() == null || entry.getKey().length() == 0) continue;
            if (entry.getValue() == null) continue;

            if (entry.getValue() instanceof Integer) {
                message.setProperty(entry.getKey(), (Integer) entry.getValue());
                continue;
            }

            if (entry.getValue() instanceof Long) {
                message.setProperty(entry.getKey(), (Long) entry.getValue());
                continue;
            }

            if (entry.getValue() instanceof Float) {
                message.setProperty(entry.getKey(), (Float) entry.getValue());
                continue;
            }

            if (entry.getValue() instanceof Double) {
                message.setProperty(entry.getKey(), (Double) entry.getValue());
                continue;
            }

            if (entry.getValue() instanceof Number) {
                message.setProperty(entry.getKey(), Double.parseDouble(entry.getValue().toString()));
                continue;
            }

            if (entry.getValue() instanceof Boolean) {
                message.setProperty(entry.getKey(), (Boolean) entry.getValue());
                continue;
            }

            if (entry.getValue() instanceof String) {
                message.setProperty(entry.getKey(), (String) entry.getValue());
                continue;
            }

            message.setProperty(entry.getKey(), entry.getValue().toString());
        }
    }

    private void error(HttpServletResponse resp, String message) {
        try {
            resp.setContentType("application/json");
            Map<String, Object> result = new HashMap<>();
            result.put("status", -1);
            result.put("error", message);
            MAPPER.writeValue(resp.getWriter(), result);
            resp.flushBuffer();
        } catch (Exception e) {
            logger.error("return message error");
        }
    }

    private void asyncSuccess(AsyncContext asyncContext, Message message) {
        try {
            Map<String, Object> result = new HashMap<>();
            result.put("status", 0);
            result.put("message", message.getMessageId());
            ServletResponse response = asyncContext.getResponse();
            response.setContentType("application/json");
            MAPPER.writeValue(response.getWriter(), result);
        } catch (Exception e) {
            logger.error("return message error {} - {}", message.getSubject(), message.getMessageId());
        } finally {
            asyncContext.complete();
        }
    }

    private void asyncError(AsyncContext asyncContext, Message message) {
        try {
            Map<String, Object> result = new HashMap<>();
            result.put("status", -1);
            result.put("error", message.getMessageId());
            ServletResponse response = asyncContext.getResponse();
            response.setContentType("application/json");
            MAPPER.writeValue(response.getWriter(), result);
        } catch (Exception e) {
            logger.error("return message error {} - {}", message.getSubject(), message.getMessageId());
        } finally {
            asyncContext.complete();
        }
    }
}
