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

package qunar.tc.qmq.consumer.pull;

import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.BrokerLoadBalance;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.broker.impl.BrokerLoadBalanceFactory;
import qunar.tc.qmq.consumer.pull.exception.SendMessageBackException;
import qunar.tc.qmq.service.exceptions.MessageException;

import java.util.Date;
import java.util.List;

/**
 * @author yiqun.fan create on 17-10-11.
 */
class DelayMessageService {
    private static final Logger LOGGER = LoggerFactory.getLogger(DelayMessageService.class);

    private static final int SEND_SUCCESS = 1;
    private static final int SEND_FAIL = -1;
    private static final int NO_BROKER = 0;

    private final BrokerService brokerService;
    private final SendMessageBack sendMessageBack;
    private final BrokerLoadBalance brokerLoadBalance;

    DelayMessageService(BrokerService brokerService, SendMessageBack sendMessageBack) {
        this.brokerService = brokerService;
        this.sendMessageBack = sendMessageBack;
        brokerLoadBalance = BrokerLoadBalanceFactory.get();
    }

    boolean sendDelayMessage(int nextRetryCount, long nextRetryTime, BaseMessage message, String group) throws MessageException {
        if (!needSendDelay(nextRetryCount, message)) return false;
        message.setProperty(BaseMessage.keys.qmq_consumerGroupName, group);
        message.setDelayTime(new Date(nextRetryTime));
        return send(message) == SEND_SUCCESS;
    }

    /**
     * 500 ms以下的不走delay message
     *
     * @param nextRetryCount
     * @param message
     * @return
     */
    private boolean needSendDelay(int nextRetryCount, BaseMessage message) {
        return message.getMaxRetryNum() >= nextRetryCount;
    }

    private int send(BaseMessage message) {
        BrokerClusterInfo brokerCluster = brokerService.getProducerBrokerCluster(ClientType.DELAY_PRODUCER, message.getSubject());
        List<BrokerGroupInfo> groups = brokerCluster.getGroups();
        if (groups == null || groups.isEmpty()) return NO_BROKER;

        BrokerGroupInfo lastSentBrokerGroup = null;
        int result = SEND_FAIL;
        for (int i = 0; i < groups.size(); i++) {
            try {
                BrokerGroupInfo brokerGroup = brokerLoadBalance.loadBalance(brokerCluster.getGroups(), lastSentBrokerGroup);
                result = doSend(message, brokerGroup);
                lastSentBrokerGroup = brokerGroup;
                if (SEND_SUCCESS == result) return result;

                LOGGER.warn("retry send delay message. {}", result);
            } catch (Exception e) {
                LOGGER.warn("retry send delay message. {}", e.getMessage());
            }
        }
        return result;
    }

    private int doSend(BaseMessage message, BrokerGroupInfo brokerGroup) {
        final SettableFuture<Integer> sendFuture = SettableFuture.create();
        final SendMessageBack.Callback callback = new SendMessageBack.Callback() {

            @Override
            public void success() {
                sendFuture.set(SEND_SUCCESS);
            }

            @Override
            public void fail(Throwable e) {
                if (e instanceof SendMessageBackException) {
                    LOGGER.warn("send delay message fail. exception: {}", e.getMessage());
                } else {
                    LOGGER.warn("send delay message fail", e);
                }
                sendFuture.set(SEND_FAIL);
            }
        };
        sendMessageBack.sendBack(brokerGroup, message, callback, ClientType.DELAY_PRODUCER);
        try {
            return sendFuture.get();
        } catch (Exception e) {
            brokerService.refresh(ClientType.DELAY_PRODUCER, message.getSubject());
            LOGGER.warn("send delay message fail. {}", e.getMessage());
            return SEND_FAIL;
        }
    }
}
