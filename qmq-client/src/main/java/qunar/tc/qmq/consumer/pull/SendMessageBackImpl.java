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

import com.google.common.collect.Lists;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.BrokerLoadBalance;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.broker.impl.BrokerLoadBalanceFactory;
import qunar.tc.qmq.common.TimerUtil;
import qunar.tc.qmq.consumer.pull.exception.SendMessageBackException;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.netty.client.NettyClient;
import qunar.tc.qmq.netty.client.ResponseFuture;
import qunar.tc.qmq.netty.exception.ClientSendException;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.MessagesPayloadHolder;
import qunar.tc.qmq.protocol.QMQSerializer;
import qunar.tc.qmq.protocol.producer.MessageProducerCode;
import qunar.tc.qmq.protocol.producer.SendResult;
import qunar.tc.qmq.util.RemotingBuilder;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static qunar.tc.qmq.protocol.CommandCode.SEND_MESSAGE;

/**
 * @author yiqun.fan create on 17-8-23.
 */
class SendMessageBackImpl implements SendMessageBack {
    private static final Logger LOGGER = LoggerFactory.getLogger(SendMessageBackImpl.class);
    private static final long SEND_MESSAGE_TIMEOUT = 5000;
    private static final int SEND_BACK_DELAY_SECONDS = 5;

    private static final NettyClient NETTY_CLIENT = NettyClient.getClient();

    private final BrokerService brokerService;

    private final BrokerLoadBalance brokerLoadBalance;

    SendMessageBackImpl(BrokerService brokerService) {
        this.brokerService = brokerService;
        this.brokerLoadBalance = BrokerLoadBalanceFactory.get();
    }

    @Override
    public void sendBack(final BrokerGroupInfo brokerGroup, BaseMessage message, final Callback callback, final ClientType clientType) {
        if (message == null) {
            if (callback != null) {
                callback.success();
            }
            return;
        }
        message.setProperty(BaseMessage.keys.qmq_createTime, new Date().getTime());
        final Datagram datagram = RemotingBuilder.buildRequestDatagram(SEND_MESSAGE, new MessagesPayloadHolder(Lists.newArrayList(message)));

        final String subject = message.getSubject();
        try {
            NETTY_CLIENT.sendAsync(brokerGroup.getMaster(), datagram, SEND_MESSAGE_TIMEOUT,
                    new ResponseFuture.Callback() {
                        @Override
                        public void processResponse(ResponseFuture responseFuture) {
                            final Datagram response = responseFuture.getResponse();
                            if (!responseFuture.isSendOk() || response == null) {
                                monitorSendError(subject, -1);
                                callback.fail(new SendMessageBackException("send fail"));
                            } else {
                                try {
                                    handleResponse(response);
                                } finally {
                                    response.release();
                                }
                            }
                        }

                        private void handleResponse(final Datagram response) {
                            final int respCode = response.getHeader().getCode();
                            final SendResult sendResult = getSendResult(response);
                            if (sendResult == null) {
                                monitorSendError(subject, 100 + respCode);
                                callback.fail(new SendMessageBackException("responseCode=" + respCode));
                                return;
                            }

                            if (respCode == CommandCode.SUCCESS && sendResult.getCode() == MessageProducerCode.SUCCESS) {
                                callback.success();
                                return;
                            }

                            if (respCode == CommandCode.BROKER_REJECT || sendResult.getCode() == MessageProducerCode.BROKER_READ_ONLY) {
                                brokerGroup.setAvailable(false);
                                brokerService.refresh(clientType, subject);
                            }

                            monitorSendError(subject, 100 + respCode);
                            callback.fail(new SendMessageBackException("responseCode=" + respCode + ", sendCode=" + sendResult.getCode()));
                        }
                    });
        } catch (ClientSendException e) {
            LOGGER.error("send message error. subject={}", subject);
            monitorSendError(subject, e.getSendErrorCode().ordinal());
            callback.fail(e);
        } catch (Exception e) {
            LOGGER.error("send message error. subject={}", subject);
            monitorSendError(subject, -1);
            callback.fail(e);
        }
    }

    private SendResult getSendResult(Datagram response) {
        try {
            Map<String, SendResult> result = QMQSerializer.deserializeSendResultMap(response.getBody());
            if (result.isEmpty()) {
                return SendResult.OK;
            }
            return result.values().iterator().next();
        } catch (Exception e) {
            LOGGER.error("sendback exception on deserializeSendResultMap.", e);
            return null;
        }
    }

    public void sendBackAndCompleteNack(final int nextRetryCount, final BaseMessage message, final AckEntry ackEntry) {
        final BrokerClusterInfo brokerCluster = brokerService.getProducerBrokerCluster(ClientType.PRODUCER, message.getSubject());
        final SendMessageBack.Callback callback = new SendMessageBack.Callback() {
            private final int retryTooMuch = brokerCluster.getGroups().size() * 2;
            private final AtomicInteger retryNumOnFail = new AtomicInteger(0);

            @Override
            public void success() {
                ackEntry.completed();
            }

            @Override
            public void fail(Throwable e) {
                if (retryNumOnFail.incrementAndGet() > retryTooMuch) {
                    if (e instanceof SendMessageBackException) {
                        LOGGER.error("send message back fail, and retry {} times after {} seconds. exception: {}", retryNumOnFail.get(), SEND_BACK_DELAY_SECONDS, e.getMessage());
                    } else {
                        LOGGER.error("send message back fail, and retry {} times after {} seconds", retryNumOnFail.get(), SEND_BACK_DELAY_SECONDS, e);
                    }
                    final SendMessageBack.Callback callback1 = this;
                    TimerUtil.newTimeout(new TimerTask() {
                        @Override
                        public void run(Timeout timeout) {
                            SendMessageBackImpl.this.sendBackAndCompleteNack(message, callback1);
                        }
                    }, SEND_BACK_DELAY_SECONDS, TimeUnit.SECONDS);
                } else {
                    if (e instanceof SendMessageBackException) {
                        LOGGER.error("send message back fail, and retry {} times after {} seconds. exception: {}", retryNumOnFail.get(), SEND_BACK_DELAY_SECONDS, e.getMessage());
                    } else {
                        LOGGER.error("send message back fail, and retry {} times after {} seconds", retryNumOnFail.get(), SEND_BACK_DELAY_SECONDS, e);
                    }
                    SendMessageBackImpl.this.sendBackAndCompleteNack(message, this);
                }
            }
        };
        final BrokerGroupInfo brokerGroup = brokerLoadBalance.loadBalance(brokerCluster.getGroups(), null);
        sendBack(brokerGroup, message, callback, ClientType.PRODUCER);
    }

    private void sendBackAndCompleteNack(final BaseMessage message, final SendMessageBack.Callback callback) {
        final BrokerClusterInfo brokerCluster = brokerService.getProducerBrokerCluster(ClientType.PRODUCER, message.getSubject());
        final BrokerGroupInfo brokerGroup = brokerLoadBalance.loadBalance(brokerCluster.getGroups(), null);
        sendBack(brokerGroup, message, callback, ClientType.PRODUCER);
    }

    private static void monitorSendError(String subject, int errorCode) {
        Metrics.counter("qmq_pull_send_msg_error", new String[]{"subject", "error"}, new String[]{subject, String.valueOf(errorCode)}).inc();
    }
}
