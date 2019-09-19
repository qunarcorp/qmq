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
import com.google.common.util.concurrent.AbstractFuture;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.ClientMetaManager;
import qunar.tc.qmq.config.PullSubjectsConfig;
import qunar.tc.qmq.consumer.pull.exception.PullException;
import qunar.tc.qmq.meta.ConsumerAllocation;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.netty.client.NettyClient;
import qunar.tc.qmq.netty.client.ResponseFuture;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.consumer.PullRequest;
import qunar.tc.qmq.protocol.consumer.PullRequestPayloadHolder;
import qunar.tc.qmq.protocol.consumer.PullRequestV10;
import qunar.tc.qmq.util.RemotingBuilder;
import qunar.tc.qmq.utils.Flags;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_ARRAY;
import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_GROUP_ARRAY;

/**
 * @author yiqun.fan create on 17-8-17.
 */
class PullService {
    private static final Logger LOGGER = LoggerFactory.getLogger(PullService.class);

    private final NettyClient client = NettyClient.getClient();
    private final ClientMetaManager clientMetaManager;

    public PullService(ClientMetaManager clientMetaManager) {
        this.clientMetaManager = clientMetaManager;
    }

    PullResult pull(final PullParam pullParam) throws ExecutionException, InterruptedException {
        final PullResultFuture result = new PullResultFuture(pullParam.getBrokerGroup());
        pull(pullParam, result);
        return result.get();
    }

    private void pull(final PullParam pullParam, final PullCallback callback) {
        String subject = pullParam.getSubject();
        final PullRequest request = buildPullRequest(pullParam);

        Datagram datagram = RemotingBuilder.buildRequestDatagram(CommandCode.PULL_MESSAGE, new PullRequestPayloadHolder(request));
        long networkTripTimeout = pullParam.getRequestTimeoutMillis();
        long pullProcessTimeout = pullParam.getTimeoutMillis();
        long responseTimeout = pullProcessTimeout < 0 ? networkTripTimeout : (networkTripTimeout + pullProcessTimeout);

        try {
            client.sendAsync(pullParam.getBrokerGroup().getMaster(), datagram, responseTimeout, new PullCallbackWrapper(request, callback));
        } catch (Exception e) {
            monitorPullError(subject, pullParam.getGroup());
            callback.onException(e);
        }
    }

    private PullRequest buildPullRequest(PullParam pullParam) {
        return new PullRequestV10(
                pullParam.getPartitionName(),
                pullParam.getGroup(),
                pullParam.getPullBatchSize(),
                pullParam.getTimeoutMillis(),
                pullParam.getConsumeOffset(),
                pullParam.getMinPullOffset(),
                pullParam.getMaxPullOffset(),
                pullParam.getConsumerId(),
                pullParam.getConsumeStrategy(),
                pullParam.getFilters(),
                pullParam.getAllocationVersion()
        );
    }

    public interface PullCallback {
        void onCompleted(short responseCode, List<BaseMessage> messages);

        void onException(Exception ex);
    }

    private final class PullCallbackWrapper implements ResponseFuture.Callback {
        private final PullRequest request;
        private final PullCallback callback;

        PullCallbackWrapper(PullRequest request, PullCallback callback) {
            this.request = request;
            this.callback = callback;
        }

        @Override
        public void processResponse(ResponseFuture responseFuture) {
            try {
                doProcessResponse(responseFuture);
            } catch (Exception e) {
                monitorPullError(request.getPartitionName(), request.getGroup());
                callback.onException(e);
            }
        }

        private void doProcessResponse(ResponseFuture responseFuture) {
            monitorPullTime(request.getPartitionName(), request.getGroup(), responseFuture.getRequestCostTime());
            if (!responseFuture.isSendOk()) {
                monitorPullError(request.getPartitionName(), request.getGroup());
                callback.onException(new PullException("send fail. opaque=" + responseFuture.getOpaque()));
                return;
            }

            final Datagram response = responseFuture.getResponse();
            if (response == null) {
                monitorPullError(request.getPartitionName(), request.getGroup());
                if (responseFuture.isTimeout()) {
                    callback.onException(new TimeoutException("pull message " + request.getPartitionName() + " timeout response: opaque=" + responseFuture.getOpaque() + ". " + responseFuture));
                } else {
                    callback.onException(new PullException("pull message " + request.getPartitionName() + "no receive response: opaque=" + responseFuture.getOpaque() + ". " + responseFuture));
                }
                return;
            }

            try {
                handleResponse(response);
            } finally {
                response.release();
            }
        }

        private void handleResponse(final Datagram response) {
            final short responseCode = response.getHeader().getCode();
            if (responseCode == CommandCode.NO_MESSAGE) {
                callback.onCompleted(responseCode, Collections.<BaseMessage>emptyList());
            } else if (responseCode != CommandCode.SUCCESS) {
                monitorPullError(request.getPartitionName(), request.getGroup());
                callback.onCompleted(responseCode, Collections.<BaseMessage>emptyList());
            } else {
                List<BaseMessage> messages = deserializeBaseMessage(response.getBody());
                if (messages == null) {
                    messages = Collections.emptyList();
                }
                monitorPullCount(request.getPartitionName(), request.getGroup(), messages.size());
                for (BaseMessage message : messages) {
                    if (message.getMaxRetryNum() < 0) {
                        String subject = message.getSubject();
                        message.setMaxRetryNum(PullSubjectsConfig.get().getMaxRetryNum(subject).get());
                    }
                }
                callback.onCompleted(responseCode, messages);
            }
        }

        private List<BaseMessage> deserializeBaseMessage(ByteBuf input) {
            if (input.readableBytes() == 0) return Collections.emptyList();
            List<BaseMessage> result = Lists.newArrayList();

            long pullLogOffset = input.readLong();
            //ignore consumer offset
            input.readLong();

            while (input.isReadable()) {
                BaseMessage message = new BaseMessage();
                byte flag = input.readByte();
                input.skipBytes(8 + 8);
                String subject = PayloadHolderUtils.readString(input);
                String messageId = PayloadHolderUtils.readString(input);
                readTags(input, message, flag);
                int bodyLen = input.readInt();
                ByteBuf body = input.readSlice(bodyLen);
                HashMap<String, Object> attrs = deserializeMapWrapper(subject, messageId, body);
                message.setMessageId(messageId);
                message.setSubject(subject);
                message.setAttrs(attrs);
                message.setProperty(BaseMessage.keys.qmq_pullOffset, pullLogOffset);

                resolveRealMessageSubject(message);

                result.add(message);

                pullLogOffset++;
            }
            return result;
        }

        private void resolveRealMessageSubject(BaseMessage message) {
            message.setSubject(message.getStringProperty(BaseMessage.keys.qmq_subject));
        }

        private void readTags(ByteBuf input, BaseMessage message, byte flag) {
            if (!Flags.hasTags(flag)) return;

            final byte tagsSize = input.readByte();
            for (int i = 0; i < tagsSize; i++) {
                final String tag = PayloadHolderUtils.readString(input);
                message.addTag(tag);
            }
        }

        private HashMap<String, Object> deserializeMapWrapper(String subject, String messageId, ByteBuf body) {
            try {
                return deserializeMap(body);
            } catch (Exception e) {
                LOGGER.error("deserialize message failed subject:{} messageId: {}", subject, messageId);
                Metrics.counter("qmq_pull_deserialize_fail_count", SUBJECT_ARRAY, new String[]{subject}).inc();
                HashMap<String, Object> result = new HashMap<>();
                result.put(BaseMessage.keys.qmq_corruptData.name(), "true");
                result.put(BaseMessage.keys.qmq_createTime.name(), new Date().getTime());
                return result;
            }
        }

        private HashMap<String, Object> deserializeMap(ByteBuf body) {
            HashMap<String, Object> map = new HashMap<>();
            while (body.isReadable(4)) {
                String key = PayloadHolderUtils.readString(body);
                String value = PayloadHolderUtils.readString(body);
                map.put(key, value);
            }
            return map;
        }
    }

    private static final class PullResultFuture extends AbstractFuture<PullResult> implements PullCallback {
        private final BrokerGroupInfo brokerGroup;

        PullResultFuture(BrokerGroupInfo brokerGroup) {
            this.brokerGroup = brokerGroup;
        }

        @Override
        public void onCompleted(short responseCode, List<BaseMessage> messages) {
            super.set(new PullResult(responseCode, messages, brokerGroup));
        }

        @Override
        public void onException(Exception ex) {
            super.setException(ex);
        }
    }

    private static void monitorPullTime(String subject, String group, long time) {
        Metrics.timer("qmq_pull_timer", SUBJECT_GROUP_ARRAY, new String[]{subject, group}).update(time, TimeUnit.MILLISECONDS);
    }

    private static void monitorPullError(String subject, String group) {
        Metrics.counter("qmq_pull_error", SUBJECT_GROUP_ARRAY, new String[]{subject, group}).inc();
    }

    private static void monitorPullCount(String subject, String group, int pullSize) {
        Metrics.counter("qmq_pull_count", SUBJECT_GROUP_ARRAY, new String[]{subject, group}).inc(pullSize);
    }
}
