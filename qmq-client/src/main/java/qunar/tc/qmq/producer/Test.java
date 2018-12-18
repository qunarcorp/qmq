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

package qunar.tc.qmq.producer;

import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.MessageProducer;
import qunar.tc.qmq.MessageSendStateListener;

import java.io.IOException;

public class Test {
    public static void main(String[] args) throws IOException {
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("spring.xml");
        context.start();

        final MessageProducer producer = context.getBean(MessageProducer.class);
        DataSourceTransactionManager transactionManager = context.getBean(DataSourceTransactionManager.class);
        TransactionTemplate template = new TransactionTemplate(transactionManager);
        template.execute(new TransactionCallback<Object>() {
            @Override
            public Object doInTransaction(TransactionStatus status) {
                Message message = producer.generateMessage("test.subject");
                message.setProperty("order", "11111");
                producer.sendMessage(message, new MessageSendStateListener() {
                    @Override
                    public void onSuccess(Message message) {
                        System.out.println("succ" + message.getMessageId());
                    }

                    @Override
                    public void onFailed(Message message) {
                        System.out.println("fail" + message.getMessageId());
                    }
                });
                return null;
            }
        });

        System.in.read();
    }
}
