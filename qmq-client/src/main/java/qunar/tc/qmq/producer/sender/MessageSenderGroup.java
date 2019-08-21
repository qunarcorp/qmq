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

package qunar.tc.qmq.producer.sender;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.metrics.MetricsConstants;
import qunar.tc.qmq.producer.SendErrorHandler;
import qunar.tc.qmq.service.exceptions.BlockMessageException;
import qunar.tc.qmq.service.exceptions.DuplicateMessageException;
import qunar.tc.qmq.service.exceptions.MessageException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author zhenyu.nie created on 2017 2017/7/5 17:26
 */
class MessageSenderGroup {

    private final static String senMessageThrowable = "qmq_client_producer_send_message_Throwable";

    private final Connection connection;
    private final List<ProduceMessage> source;

    MessageSenderGroup(Connection connection) {
        this.connection = connection;
        this.source = new ArrayList<>();
    }

    public void sendAsync(SendErrorHandler errorHandler) {
        try {
            ListenableFuture<Map<String, MessageException>> future = connection.sendAsync(source);
            Futures.addCallback(future, new FutureCallback<Map<String, MessageException>>() {
                @Override
                public void onSuccess(Map<String, MessageException> result) {
                    processResult(result, errorHandler);
                }

                @Override
                public void onFailure(Throwable t) {
                    Exception ex;
                    if (!(t instanceof Exception)) {
                        ex = new RuntimeException(t);
                    } else {
                        ex = (Exception) t;
                    }
                    processError(ex, errorHandler);
                }
            });
        } catch (Exception e) {
            processError(e, errorHandler);
        }
    }

    public void send(SendErrorHandler errorHandler) {
        Map<String, MessageException> result;
        try {
            result = connection.send(source);
        } catch (Exception e) {
            processError(e, errorHandler);
            return;
        }

        processResult(result, errorHandler);
    }

    private void processError(Exception e, SendErrorHandler errorHandler) {
        for (ProduceMessage pm : source) {
            Metrics.counter(senMessageThrowable, MetricsConstants.SUBJECT_ARRAY, new String[]{pm.getSubject()}).inc();
            errorHandler.error(pm, e);
        }
        errorHandler.postHandle(source);
    }

    private void processResult(Map<String, MessageException> result, SendErrorHandler errorHandler) {
        if (result == null) {
            for (ProduceMessage pm : source) {
                Metrics.counter(senMessageThrowable, MetricsConstants.SUBJECT_ARRAY, new String[]{pm.getSubject()});
                errorHandler.error(pm, new MessageException(pm.getMessageId(), "return null"));
            }
            return;
        }

        if (result.isEmpty())
            result = Collections.emptyMap();

        for (ProduceMessage pm : source) {
            MessageException ex = result.get(pm.getMessageId());
            if (ex == null || ex instanceof DuplicateMessageException) {
                errorHandler.finish(pm, ex);
            } else {
                //如果是消息被拒绝，说明broker已经限速，不立即重试;
                if (ex.isBrokerBusy()) {
                    errorHandler.failed(pm, ex);
                } else if (ex instanceof BlockMessageException) {
                    //如果是block的,证明还没有被授权,也不重试,task也不重试,需要手工恢复
                    errorHandler.block(pm, ex);
                } else {
                    Metrics.counter(senMessageThrowable, MetricsConstants.SUBJECT_ARRAY, new String[]{pm.getSubject()});
                    errorHandler.error(pm, ex);
                }
            }
        }

        errorHandler.postHandle(source);
    }

    void addMessage(ProduceMessage source) {
        this.source.add(source);
    }
}
