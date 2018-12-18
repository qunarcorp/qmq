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

package qunar.tc.qmq.producer.tx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.MessageProducer;
import qunar.tc.qmq.MessageStore;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.TransactionListener;

import java.util.Collections;
import java.util.List;
import java.util.Stack;

/**
 * Created by zhaohui.yu
 * 10/29/16
 */
class DefaultTransactionListener implements TransactionListener {
    private static final Logger logger = LoggerFactory.getLogger(MessageProducer.class);

    private final ThreadLocal<Stack<TransactionMessageHolder>> resource = new ThreadLocal<Stack<TransactionMessageHolder>>() {
        @Override
        protected Stack<TransactionMessageHolder> initialValue() {
            return new Stack<>();
        }
    };

    private ThreadLocal<TransactionMessageHolder> holder = new ThreadLocal<>();

    public void beginTransaction(MessageStore store) {
        TransactionMessageHolder current = holder.get();
        if (current == null) {
            holder.set(new TransactionMessageHolder(store));
        }
    }

    @Override
    public void addMessage(ProduceMessage message) {
        TransactionMessageHolder current = holder.get();
        if (current == null) return;
        current.insertMessage(message);
    }

    @Override
    public void beforeCommit() {
        List<ProduceMessage> list = get();
        for (ProduceMessage msg : list) {
            msg.save();
        }
    }

    @Override
    public void afterCommit() {
        List<ProduceMessage> list = remove();

        for (int i = 0; i < list.size(); ++i) {
            ProduceMessage msg = list.get(i);
            try {
                msg.send();
            } catch (Throwable t) {
                logger.error("消息发送失败{}", msg.getMessageId(), t);
            }
        }
    }

    @Override
    public void afterCompletion() {
        List<ProduceMessage> list = remove();
        for (int i = 0; i < list.size(); ++i) {
            ProduceMessage msg = list.get(i);
            logger.info("事务提交失败, 消息({})被忽略.subject:{}", msg.getMessageId(), msg.getSubject());
        }
    }

    @Override
    public void suspend() {
        TransactionMessageHolder current = holder.get();
        if (current == null) return;
        holder.remove();
        resource.get().push(current);
    }

    @Override
    public void resume() {
        holder.set(resource.get().pop());
    }

    private List<ProduceMessage> get() {
        TransactionMessageHolder current = holder.get();
        if (current == null) return Collections.emptyList();
        return current.get();
    }

    private List<ProduceMessage> remove() {
        TransactionMessageHolder current = holder.get();
        holder.remove();
        if (current == null) return Collections.emptyList();
        return current.get();
    }
}
