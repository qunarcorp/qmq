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

import qunar.tc.qmq.ProduceMessage;
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

    private final Connection connection;
    private final SendErrorHandler errorHandler;

    private final List<ProduceMessage> source;

    MessageSenderGroup(SendErrorHandler errorHandler, Connection connection) {
        this.errorHandler = errorHandler;
        this.connection = connection;
        this.source = new ArrayList<>();
    }

    public void send() {
        Map<String, MessageException> map;
        try {
            map = connection.send(source);
        } catch (Exception e) {
            for (ProduceMessage pm : source) {
                errorHandler.error(pm, e);
            }
            return;
        }

        if (map == null) {
            for (ProduceMessage pm : source) {
                errorHandler.error(pm, new MessageException(pm.getMessageId(), "return null"));
            }
            return;
        }

        if (map.isEmpty())
            map = Collections.emptyMap();

        for (ProduceMessage pm : source) {
            MessageException ex = map.get(pm.getMessageId());
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
                    errorHandler.error(pm, ex);
                }
            }
        }
    }

    void addMessage(ProduceMessage source) {
        this.source.add(source);
    }
}
