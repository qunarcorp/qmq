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

import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.consumer.pull.exception.AckException;
import qunar.tc.qmq.meta.ConsumerAllocation;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.metrics.QmqCounter;
import qunar.tc.qmq.netty.client.NettyClient;
import qunar.tc.qmq.netty.client.ResponseFuture;
import qunar.tc.qmq.netty.exception.ClientSendException;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.consumer.AckRequest;
import qunar.tc.qmq.protocol.consumer.AckRequestPayloadHolder;
import qunar.tc.qmq.util.RemotingBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_GROUP_ARRAY;

/**
 * @author yiqun.fan create on 17-8-18.
 */
class DefaultAckService implements AckService {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultAckService.class);

    private static final QmqCounter GET_PULL_OFFSET_ERROR = Metrics.counter("qmq_pull_getpulloffset_error");

    private static final long ACK_REQUEST_TIMEOUT_MILLIS = 10 * 1000;

    private final NettyClient client = NettyClient.getClient();
    private final BrokerService brokerService;
    private final SendMessageBack sendMessageBack;
    private final DelayMessageService delayMessageService;

    private String clientId;

    DefaultAckService(BrokerService brokerService, SendMessageBack sendMessageBack) {
        this.brokerService = brokerService;
        this.sendMessageBack = sendMessageBack;
        this.delayMessageService = new DelayMessageService(brokerService, sendMessageBack);
    }

    @Override
    public List<PulledMessage> buildPulledMessages(PullParam pullParam, PullResult pullResult, AckSendQueue sendQueue, AckHook ackHook, PulledMessageFilter filter) {
        final List<BaseMessage> pulledMessages = pullResult.getMessages();
        final List<PulledMessage> result = new ArrayList<>(pulledMessages.size());
        final List<PulledMessage> ignoreMessages = new ArrayList<>();
        final List<AckEntry> ackEntries = new ArrayList<>(pulledMessages.size());

        long prevPullOffset = 0;
        AckEntry preAckEntry = null;

        for (BaseMessage message : pulledMessages) {
            final long pullOffset = getOffset(message);
            if (pullOffset < prevPullOffset) {
                monitorGetPullOffsetError(message);
                continue;
            }

            prevPullOffset = pullOffset;
            AckEntry ackEntry = new AckEntry(sendQueue, pullOffset, delayMessageService);
            ackEntries.add(ackEntry);
            if (preAckEntry != null) {
                preAckEntry.setNext(ackEntry);
            }
            preAckEntry = ackEntry;

            PulledMessage pulledMessage = new PulledMessage(message, ackEntry, ackHook);
            if (filter.filter(pulledMessage)) {
                result.add(pulledMessage);
            } else {
                ignoreMessages.add(pulledMessage);
            }

            pulledMessage.setSubject(pullParam.getConsumeParam().getSubject());
            pulledMessage.setProperty(BaseMessage.keys.qmq_consumerGroupName, pullParam.getGroup());
        }
        sendQueue.append(ackEntries);
        ackIgnoreMessages(ignoreMessages);
        preAckOnDemand(result, pullParam.isConsumeMostOnce());
        return result;
    }

    private void preAckOnDemand(List<PulledMessage> messages, boolean isConsumeMostOnce) {
        for (PulledMessage message : messages) {
            if (isConsumeMostOnce) {
                message.ackWithTrace(null);
            }
        }
    }

    private void ackIgnoreMessages(List<PulledMessage> ignoreMessages) {
        for (PulledMessage message : ignoreMessages) {
            message.ackWithTrace(null);
        }
    }


    private long getOffset(BaseMessage message) {
        Object offsetObj = message.getProperty(BaseMessage.keys.qmq_pullOffset);
        if (offsetObj == null) {
            return -1;
        }
        try {
            return Long.parseLong(offsetObj.toString());
        } catch (Exception e) {
            return -1;
        }
    }

    private static void monitorGetPullOffsetError(BaseMessage message) {
        LOGGER.error("lost pull offset. msgId=" + message.getMessageId());
        GET_PULL_OFFSET_ERROR.inc();
    }

    public void sendAck(BrokerGroupInfo brokerGroup, String subject, String consumerGroup, ConsumeStrategy consumeStrategy, AckSendEntry ack, SendAckCallback callback) {
        AckRequest request = buildAckRequest(subject, consumerGroup, consumeStrategy, ack);
        Datagram datagram = RemotingBuilder.buildRequestDatagram(CommandCode.ACK_REQUEST, new AckRequestPayloadHolder(request));
        sendRequest(brokerGroup, subject, consumerGroup, request, datagram, callback);
    }

    private AckRequest buildAckRequest(String subject, String consumerGroup, ConsumeStrategy consumeStrategy, AckSendEntry ack) {
        return new AckRequest(
                subject,
                consumerGroup,
                clientId,
                ack.getPullOffsetBegin(),
                ack.getPullOffsetLast(),
                (byte) (Objects.equals(consumeStrategy, ConsumeStrategy.EXCLUSIVE) ? 1 : 0)
        );
    }

    private void sendRequest(BrokerGroupInfo brokerGroup, String subject, String consumerGroup, AckRequest request, Datagram datagram, SendAckCallback callback) {
        try {
            client.sendAsync(brokerGroup.getMaster(), datagram, ACK_REQUEST_TIMEOUT_MILLIS, new AckResponseCallback(request, callback, brokerService));
        } catch (ClientSendException e) {
            ClientSendException.SendErrorCode errorCode = e.getSendErrorCode();
            monitorAckError(subject, consumerGroup, errorCode.ordinal());
            callback.fail(e);
        } catch (Exception e) {
            monitorAckError(subject, consumerGroup, -1);
            callback.fail(e);
        }
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    private static final class AckResponseCallback implements ResponseFuture.Callback {
        private final AckRequest request;
        private final SendAckCallback sendAckCallback;
        private final BrokerService brokerService;

        AckResponseCallback(AckRequest request, SendAckCallback sendAckCallback, BrokerService brokerService) {
            this.request = request;
            this.sendAckCallback = sendAckCallback;
            this.brokerService = brokerService;
        }

        @Override
        public void processResponse(ResponseFuture responseFuture) {
            monitorAckTime(request.getSubject(), request.getGroup(), responseFuture.getRequestCostTime());

            Datagram response = responseFuture.getResponse();
            if (!responseFuture.isSendOk() || response == null) {
                monitorAckError(request.getSubject(), request.getGroup(), -1);
                sendAckCallback.fail(new AckException("send fail"));
                this.brokerService.refresh(ClientType.CONSUMER, request.getSubject(), request.getGroup());
                return;
            }
            final short responseCode = response.getHeader().getCode();
            if (responseCode == CommandCode.SUCCESS) {
                sendAckCallback.success();
            } else {
                monitorAckError(request.getSubject(), request.getGroup(), 100 + responseCode);
                this.brokerService.refresh(ClientType.CONSUMER, request.getSubject(), request.getGroup());
                sendAckCallback.fail(new AckException("responseCode: " + responseCode));
            }
        }
    }

    private static void monitorAckTime(String subject, String group, long time) {
        Metrics.timer("qmq_pull_ack_timer", SUBJECT_GROUP_ARRAY, new String[]{subject, group}).update(time, TimeUnit.MILLISECONDS);
    }

    private static void monitorAckError(String subject, String group, int errorCode) {
        LOGGER.error("ack error. subject={}, group={}, errorCode={}", subject, group, errorCode);
        Metrics.counter("qmq_pull_ack_error", SUBJECT_GROUP_ARRAY, new String[]{subject, group}).inc();
    }

    private static final Joiner senderKeyJoiner = Joiner.on("$");

    private static String buildSenderKey(String subject, String consumerGroup, String brokerGroup, String partitionName) {
        return senderKeyJoiner.join(subject, consumerGroup, brokerGroup, partitionName);
    }
}
