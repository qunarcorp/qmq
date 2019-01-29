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

package qunar.tc.qmq.demo.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.MessageProducer;
import qunar.tc.qmq.MessageSendStateListener;
import qunar.tc.qmq.demo.dao.OrderRepository;
import qunar.tc.qmq.demo.model.Order;

import javax.annotation.Resource;
import javax.transaction.Transactional;

@Service
public class OrderService {
    private static final Logger LOG = LoggerFactory.getLogger(OrderService.class);

    @Resource
    private MessageProducer producer;

    @Resource
    private OrderRepository orderRepository;

    @Transactional
    public void placeOrder(Order order) {
        final Message message = producer.generateMessage("order.changed");
        message.setProperty("orderId", order.getOrderId());
        message.setProperty("name", order.getName());
        producer.sendMessage(message, new MessageSendStateListener() {
            @Override
            public void onSuccess(Message message) {
                LOG.info("send message success: {}", message.getMessageId());
            }

            @Override
            public void onFailed(Message message) {
                LOG.error("send message failed: {}", message.getMessageId());
            }
        });

        orderRepository.save(order);
    }
}
