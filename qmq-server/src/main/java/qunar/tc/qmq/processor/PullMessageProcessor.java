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

package qunar.tc.qmq.processor;

import com.google.common.base.CharMatcher;
import com.google.common.base.Strings;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.PullMessageResult;
import qunar.tc.qmq.concurrent.ActorSystem;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.consumer.SubscriberStatusChecker;
import qunar.tc.qmq.monitor.QMon;
import qunar.tc.qmq.protocol.*;
import qunar.tc.qmq.protocol.consumer.PullRequest;
import qunar.tc.qmq.stats.BrokerStats;
import qunar.tc.qmq.store.ConsumerLogWroteEvent;
import qunar.tc.qmq.store.MessageStoreWrapper;
import qunar.tc.qmq.store.SegmentBuffer;
import qunar.tc.qmq.store.event.FixedExecOrderEventBus;
import qunar.tc.qmq.util.RemotingBuilder;
import qunar.tc.qmq.utils.ConsumerGroupUtils;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static qunar.tc.qmq.protocol.RemotingHeader.VERSION_8;

/**
 * @author yunfeng.yang
 * @since 2017/7/4
 */
public class PullMessageProcessor extends AbstractRequestProcessor implements FixedExecOrderEventBus.Listener<ConsumerLogWroteEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(PullMessageProcessor.class);

    private static final int DEFAULT_NETWORK_TIMEOUT = 5000;
    private static final int DEFAULT_MAX_LOAD_TIME = 2500;

    private static final CharMatcher ILLEGAL_MATCHER = CharMatcher.anyOf("/\r\n");

    private final HashedWheelTimer timer = new HashedWheelTimer(50, TimeUnit.MILLISECONDS);
    private final DynamicConfig config;
    private final ActorSystem actorSystem;
    private final SubscriberStatusChecker subscriberStatusChecker;
    private final PullMessageWorker pullMessageWorker;

    public PullMessageProcessor(final DynamicConfig config,
                                final ActorSystem actorSystem,
                                final MessageStoreWrapper messageStoreWrapper,
                                final SubscriberStatusChecker subscriberStatusChecker) {
        this.config = config;
        this.actorSystem = actorSystem;
        this.subscriberStatusChecker = subscriberStatusChecker;
        this.pullMessageWorker = new PullMessageWorker(messageStoreWrapper, actorSystem);
        this.timer.start();
    }

    @Override
    public CompletableFuture<Datagram> processRequest(final ChannelHandlerContext ctx, final RemotingCommand command) {
        final PullRequest pullRequest = deserializePullRequest(command.getHeader().getVersion(), command.getBody());

        BrokerStats.getInstance().getLastMinutePullRequestCount().add(1);
        QMon.pullRequestCountInc(pullRequest.getSubject(), pullRequest.getGroup());

        if (!checkAndRepairPullRequest(pullRequest)) {
            return CompletableFuture.completedFuture(crateErrorParamResult(command));
        }

        subscribe(pullRequest);

        final PullEntry entry = new PullEntry(pullRequest, command.getHeader(), ctx);
        pullMessageWorker.pull(entry);
        return null;
    }

    // TODO(keli.wang): how to handle broadcast subscriber correctly?
    private void subscribe(PullRequest pullRequest) {
        if (pullRequest.isBroadcast()) return;

        final String subject = pullRequest.getSubject();
        final String group = pullRequest.getGroup();
        final String consumerId = pullRequest.getConsumerId();
        subscriberStatusChecker.addSubscriber(subject, group, consumerId);
        subscriberStatusChecker.heartbeat(consumerId, subject, group);
    }

    private boolean checkAndRepairPullRequest(PullRequest pullRequest) {
        final String subject = pullRequest.getSubject();
        final String group = pullRequest.getGroup();
        final String consumerId = pullRequest.getConsumerId();

        if (Strings.isNullOrEmpty(subject)
                || Strings.isNullOrEmpty(group)
                || Strings.isNullOrEmpty(consumerId)
                || hasIllegalPart(subject, group, consumerId)) {
            QMon.pullParamErrorCountInc(subject, group);
            LOG.warn("receive pull request param error, request: {}", pullRequest);
            return false;
        }

        if (pullRequest.getRequestNum() <= 0) {
            QMon.nonPositiveRequestNumCountInc(subject, group);

            if (config.getBoolean("PullMessageProcessor.AllowNonPositiveRequestNum", false)) {
                pullRequest.setRequestNum(20);
            } else {
                return false;
            }
        }

        if (pullRequest.getRequestNum() > 10000) {
            pullRequest.setRequestNum(10000);
        }

        return true;
    }

    private boolean hasIllegalPart(final String... parts) {
        for (final String part : parts) {
            if (ILLEGAL_MATCHER.matchesAnyOf(part)) {
                return true;
            }
        }

        return false;
    }

    private Datagram crateErrorParamResult(RemotingCommand command) {
        return RemotingBuilder.buildEmptyResponseDatagram(CommandCode.NO_MESSAGE, command.getHeader());
    }

    private PullRequest deserializePullRequest(final int version, ByteBuf input) {
        String prefix = PayloadHolderUtils.readString(input);
        String group = PayloadHolderUtils.readString(input);
        String consumerId = PayloadHolderUtils.readString(input);
        int requestNum = input.readInt();
        long offset = input.readLong();
        long pullOffsetBegin = input.readLong();
        long pullOffsetLast = input.readLong();
        long timeout = input.readLong();
        byte broadcast = input.readByte();

        PullRequest request = new PullRequest();
        deserializeTags(request, version, input);
        request.setSubject(prefix);
        request.setGroup(group);
        request.setConsumerId(consumerId);
        request.setRequestNum(requestNum);
        request.setOffset(offset);
        request.setPullOffsetBegin(pullOffsetBegin);
        request.setPullOffsetLast(pullOffsetLast);
        request.setTimeoutMillis(timeout);
        request.setBroadcast(broadcast != 0);
        return request;
    }

    private void deserializeTags(PullRequest request, int version, ByteBuf input) {
        if (version < VERSION_8) return;

        int tagTypeCode = input.readShort();
        final byte tagSize = input.readByte();
        List<byte[]> tags = new ArrayList<>(tagSize);
        for (int i = 0; i < tagSize; i++) {
            int len = input.readShort();
            byte[] bs = new byte[len];
            input.readBytes(bs);
            tags.add(bs);
        }
        request.setTagTypeCode(tagTypeCode);
        request.setTags(tags);
    }

    @Override
    public void onEvent(final ConsumerLogWroteEvent e) {
        if (!e.isSuccess() || Strings.isNullOrEmpty(e.getSubject())) {
            return;
        }
        pullMessageWorker.remindNewMessages(e.getSubject());
    }

    class PullEntry implements TimerTask {
        final String subject;
        final String group;
        final long pullBegin;
        final RemotingHeader requestHeader;
        final PullRequest pullRequest;

        private final ChannelHandlerContext ctx;
        private final long deadline;
        private volatile boolean isTimeout;

        PullEntry(PullRequest pullRequest, RemotingHeader requestHeader, ChannelHandlerContext ctx) {
            this.pullRequest = pullRequest;
            this.subject = pullRequest.getSubject();
            this.group = pullRequest.getGroup();
            this.requestHeader = requestHeader;
            this.ctx = ctx;
            this.pullBegin = System.currentTimeMillis();
            this.deadline = pullBegin + DEFAULT_NETWORK_TIMEOUT + Math.max(pullRequest.getTimeoutMillis(), 0);
        }

        @Override
        public void run(Timeout timeout) {
            isTimeout = true;
            QMon.pullTimeOutCountInc(subject, group);
            actorSystem.resume(ConsumerGroupUtils.buildConsumerGroupKey(subject, group));
        }

        boolean isTimeout() {
            return isTimeout;
        }

        boolean expired() {
            return deadline - System.currentTimeMillis() < DEFAULT_MAX_LOAD_TIME;
        }

        boolean isInValid() {
            Channel channel = ctx.channel();
            return channel == null || !channel.isActive();
        }

        boolean isPullOnce() {
            return pullRequest.getTimeoutMillis() < 0;
        }

        boolean setTimerOnDemand() {
            if (pullRequest.getTimeoutMillis() <= 0) return false;

            long elapsed = System.currentTimeMillis() - pullBegin;
            long wait = pullRequest.getTimeoutMillis() - elapsed;
            if (wait > 0) {
                timer.newTimeout(this, wait, TimeUnit.MILLISECONDS);
                return true;
            }
            return false;
        }

        void processNoMessageResult() {
            QMon.pulledNoMessagesCountInc(subject, group);

            final Datagram response = RemotingBuilder.buildEmptyResponseDatagram(CommandCode.NO_MESSAGE, requestHeader);
            ctx.writeAndFlush(response).addListener(future -> monitorPullProcessTime());
        }

        void processMessageResult(PullMessageResult pullMessageResult) {
            if (pullMessageResult.getMessageNum() <= 0) {
                processNoMessageResult();
                return;
            }

            QMon.pulledMessagesCountInc(subject, group, pullMessageResult.getMessageNum());
            QMon.pulledMessageBytesCountInc(subject, group, pullMessageResult.getBufferTotalSize());
            final Datagram response = RemotingBuilder.buildResponseDatagram(CommandCode.SUCCESS, requestHeader, toPayloadHolder(pullMessageResult, requestHeader));
            ctx.writeAndFlush(response).addListener(future -> monitorPullProcessTime());
        }

        private void monitorPullProcessTime() {
            QMon.pullProcessTime(subject, group, System.currentTimeMillis() - pullBegin);
        }

        private PullMessageResultPayloadHolder toPayloadHolder(final PullMessageResult result, final RemotingHeader requestHeader) {
            final long start = System.currentTimeMillis();

            try {
                int payloadSize = 8 + 8 + result.getBufferTotalSize();
                final ByteBuffer output = ByteBuffer.allocate(payloadSize);
                output.putLong(result.getPullLogOffset());
                output.putLong(-1);

                final List<SegmentBuffer> buffers = result.getBuffers();
                for (final SegmentBuffer buffer : buffers) {
                    try {
                        output.put(buffer.getBuffer());
                    } finally {
                        buffer.release();
                    }
                }
                if (output.hasRemaining()) {
                    //将流中原来tags所占的区间释放
                    return new PullMessageResultPayloadHolder(Arrays.copyOf(output.array(), output.position()));
                } else {
                    return new PullMessageResultPayloadHolder(output.array());
                }
            } finally {
                QMon.readPullResultAsBytesElapsed(subject, group, System.currentTimeMillis() - start);
            }
        }

    }

    class PullMessageResultPayloadHolder implements PayloadHolder {
        private final byte[] data;

        PullMessageResultPayloadHolder(final byte[] data) {
            this.data = data;
        }

        @Override
        public void writeBody(ByteBuf out) {
            out.writeBytes(data);
        }
    }
}