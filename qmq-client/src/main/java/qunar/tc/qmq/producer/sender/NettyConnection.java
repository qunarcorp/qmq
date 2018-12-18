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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.BrokerLoadBalance;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.broker.impl.PollBrokerLoadBalance;
import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.metrics.QmqCounter;
import qunar.tc.qmq.metrics.QmqTimer;
import qunar.tc.qmq.netty.exception.*;
import qunar.tc.qmq.protocol.*;
import qunar.tc.qmq.protocol.producer.MessageProducerCode;
import qunar.tc.qmq.protocol.producer.SendResult;
import qunar.tc.qmq.service.exceptions.BlockMessageException;
import qunar.tc.qmq.service.exceptions.DuplicateMessageException;
import qunar.tc.qmq.service.exceptions.MessageException;
import qunar.tc.qmq.tracing.TraceUtil;
import qunar.tc.qmq.util.RemotingBuilder;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_ARRAY;

/**
 * @author zhenyu.nie created on 2017 2017/7/5 15:08
 */
class NettyConnection implements Connection {
    private final String subject;
    private final ClientType clientType;
    private final NettyProducerClient producerClient;
    private final BrokerService brokerService;

    private volatile BrokerGroupInfo lastSentBroker;

    private final BrokerLoadBalance brokerLoadBalance = PollBrokerLoadBalance.getInstance();

    private final QmqCounter sendMessageCountMetrics;
    private final QmqTimer sendMessageTimerMetrics;

    NettyConnection(String subject, ClientType clientType, NettyProducerClient producerClient, BrokerService brokerService) {
        this.subject = subject;
        this.clientType = clientType;
        this.producerClient = producerClient;
        this.brokerService = brokerService;

        sendMessageCountMetrics = Metrics.counter("qmq_client_send_msg_count", SUBJECT_ARRAY, new String[]{subject});
        sendMessageTimerMetrics = Metrics.timer("qmq_client_send_msg_timer");
    }

    public void init() {
        brokerService.refresh(clientType, subject);
    }

    @Override
    public Map<String, MessageException> send(List<ProduceMessage> messages) throws ClientSendException, RemoteException, BrokerRejectException {
        sendMessageCountMetrics.inc(messages.size());
        long start = System.currentTimeMillis();
        try {
            BrokerClusterInfo cluster = brokerService.getClusterBySubject(clientType, subject);
            BrokerGroupInfo target = brokerLoadBalance.loadBalance(cluster, lastSentBroker);
            if (target == null) {
                throw new ClientSendException(ClientSendException.SendErrorCode.CREATE_CHANNEL_FAIL);
            }

            lastSentBroker = target;
            Datagram response = doSend(target, messages);
            RemotingHeader responseHeader = response.getHeader();
            int code = responseHeader.getCode();
            switch (code) {
                case CommandCode.SUCCESS:
                    return process(target, response);
                case CommandCode.BROKER_REJECT:
                    handleSendReject(target);
                    throw new BrokerRejectException("");
                default:
                    throw new RemoteException();
            }
        } finally {
            sendMessageTimerMetrics.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
        }
    }

    private void handleSendReject(BrokerGroupInfo target) {
        if (target != null) {
            target.setAvailable(false);
        }
        this.brokerService.refresh(ClientType.PRODUCER, subject);
    }

    private Map<String, MessageException> process(BrokerGroupInfo target, Datagram response) throws RemoteResponseUnreadableException {
        ByteBuf buf = response.getBody();
        try {
            if (buf == null || !buf.isReadable()) {
                return Collections.emptyMap();
            }
            Map<String, SendResult> resultMap = QMQSerializer.deserializeSendResultMap(buf);
            boolean brokerReject = false;
            Map<String, MessageException> map = Maps.newHashMapWithExpectedSize(resultMap.size());
            for (Map.Entry<String, SendResult> entry : resultMap.entrySet()) {
                String messageId = entry.getKey();
                SendResult result = entry.getValue();
                switch (result.getCode()) {
                    case MessageProducerCode.SUCCESS:
                        break;
                    case MessageProducerCode.MESSAGE_DUPLICATE:
                        map.put(messageId, new DuplicateMessageException(messageId));
                        break;
                    case MessageProducerCode.BROKER_BUSY:
                        map.put(messageId, new MessageException(messageId, MessageException.BROKER_BUSY));
                        break;
                    case MessageProducerCode.BROKER_READ_ONLY:
                        brokerReject = true;
                        map.put(messageId, new BrokerRejectException(messageId));
                        break;
                    case MessageProducerCode.SUBJECT_NOT_ASSIGNED:
                        map.put(messageId, new SubjectNotAssignedException(messageId));
                        break;
                    case MessageProducerCode.BLOCK:
                        map.put(messageId, new BlockMessageException(messageId));
                        break;
                    default:
                        map.put(messageId, new MessageException(messageId, result.getRemark()));
                        break;
                }
            }

            if (brokerReject) {
                handleSendReject(target);
            }
            return map;
        } finally {
            response.release();
        }
    }

    private Datagram doSend(BrokerGroupInfo target, List<ProduceMessage> messages) throws ClientSendException, RemoteTimeoutException {
        try {
            Datagram datagram = buildDatagram(messages);
            TraceUtil.setTag("broker", target.getGroupName());
            Datagram result = producerClient.sendMessage(target, datagram);
            target.markSuccess();
            return result;
        } catch (ClientSendException | RemoteTimeoutException e) {
            target.markFailed();
            Metrics.counter("qmq_client_send_msg_error").inc(messages.size());
            throw e;
        } catch (Exception e) {
            target.markFailed();
            Metrics.counter("qmq_client_send_msg_error").inc(messages.size());
            throw new RuntimeException(e);
        }
    }

    private Datagram buildDatagram(List<ProduceMessage> messages) {
        final List<BaseMessage> baseMessages = Lists.newArrayListWithCapacity(messages.size());
        for (ProduceMessage message : messages) {
            baseMessages.add((BaseMessage) message.getBase());
        }
        return RemotingBuilder.buildRequestDatagram(CommandCode.SEND_MESSAGE, new MessagesPayloadHolder(baseMessages));
    }

    @Override
    public String url() {
        return "newqmq://" + subject;
    }

    @Override
    public void destroy() {
    }
}
