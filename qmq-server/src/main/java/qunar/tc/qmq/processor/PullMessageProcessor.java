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
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import io.netty.util.HashedWheelTimer;
import io.netty.util.ReferenceCounted;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.PullMessageResult;
import qunar.tc.qmq.concurrent.ActorSystem;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.consumer.SubscriberStatusChecker;
import qunar.tc.qmq.monitor.QMon;
import qunar.tc.qmq.order.ExclusiveMessageLockManager;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.RemotingCommand;
import qunar.tc.qmq.protocol.RemotingHeader;
import qunar.tc.qmq.protocol.consumer.PullRequest;
import qunar.tc.qmq.protocol.consumer.PullRequestSerde;
import qunar.tc.qmq.stats.BrokerStats;
import qunar.tc.qmq.store.ConsumerLogWroteEvent;
import qunar.tc.qmq.store.MessageStoreWrapper;
import qunar.tc.qmq.store.buffer.Buffer;
import qunar.tc.qmq.store.event.FixedExecOrderEventBus;
import qunar.tc.qmq.util.RemotingBuilder;
import qunar.tc.qmq.utils.ConsumerGroupUtils;
import qunar.tc.qmq.utils.Flags;
import qunar.tc.qmq.utils.HeaderSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static qunar.tc.qmq.protocol.RemotingHeader.VERSION_8;
import static qunar.tc.qmq.util.RemotingBuilder.buildResponseHeader;

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
    private final PullRequestSerde pullRequestSerde;
    private final ExclusiveMessageLockManager exclusiveMessageLockManager;

    public PullMessageProcessor(final DynamicConfig config,
                                final ActorSystem actorSystem,
                                final MessageStoreWrapper messageStoreWrapper,
                                final SubscriberStatusChecker subscriberStatusChecker,
                                final ExclusiveMessageLockManager exclusiveMessageLockManager) {
        this.config = config;
        this.actorSystem = actorSystem;
        this.subscriberStatusChecker = subscriberStatusChecker;
        this.exclusiveMessageLockManager = exclusiveMessageLockManager;
        this.pullMessageWorker = new PullMessageWorker(messageStoreWrapper, actorSystem);
        this.pullRequestSerde = new PullRequestSerde();
        this.timer.start();
    }

    @Override
    public CompletableFuture<Datagram> processRequest(final ChannelHandlerContext ctx, final RemotingCommand command) {
        final PullRequest pullRequest = pullRequestSerde.read(command.getHeader().getVersion(), command.getBody());

        BrokerStats.getInstance().getLastMinutePullRequestCount().add(1);
        QMon.pullRequestCountInc(pullRequest.getPartitionName(), pullRequest.getGroup());

        if (!checkAndRepairPullRequest(pullRequest)) {
            return CompletableFuture.completedFuture(crateEmptyResult(command));
        }

        if (pullRequest.isExclusiveConsume()) {
            String partitionName = pullRequest.getPartitionName();
            String group = pullRequest.getGroup();
            String consumerId = pullRequest.getConsumerId();
            if (!exclusiveMessageLockManager.acquireLock(partitionName, group, consumerId)) {
                // 获取锁失败
                CompletableFuture<Datagram> future = new CompletableFuture<>();
                future.completeExceptionally(new UnsupportedOperationException(String.format("acquire lock failed %s %s", partitionName, group)));
                return future;
            }
        }

        subscribe(pullRequest);

        final PullEntry entry = new PullEntry(pullRequest, command.getHeader(), ctx);
        pullMessageWorker.pull(entry);
        return null;
    }

    private void subscribe(PullRequest pullRequest) {
        if (pullRequest.isExclusiveConsume()) return;

        final String partitionName = pullRequest.getPartitionName();
        final String group = pullRequest.getGroup();
        final String consumerId = pullRequest.getConsumerId();
        subscriberStatusChecker.addSubscriber(partitionName, group, consumerId);
        subscriberStatusChecker.heartbeat(consumerId, partitionName, group);
    }

    private boolean checkAndRepairPullRequest(PullRequest pullRequest) {
        final String subject = pullRequest.getPartitionName();
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

    private Datagram crateEmptyResult(RemotingCommand command) {
        return RemotingBuilder.buildEmptyResponseDatagram(CommandCode.NO_MESSAGE, command.getHeader());
    }

    @Override
    public void onEvent(final ConsumerLogWroteEvent e) {
        if (!e.isSuccess() || Strings.isNullOrEmpty(e.getSubject())) {
            return;
        }
        pullMessageWorker.remindNewMessages(e.getSubject());
    }

    static class DataTransfer implements FileRegion {

        private final ByteBuffer[] buffers;
        private final ByteBuf header;
        private final ByteBuf payload;
        private final long count;
        private long transferred;

        public DataTransfer(RemotingHeader requestHeader, ByteBuf payload) {
            this.payload = payload;
            this.header = HeaderSerializer.serialize(requestHeader, payload.readableBytes(), 0);

            this.buffers = new ByteBuffer[2];
            this.buffers[0] = header.nioBuffer();
            this.buffers[1] = payload.nioBuffer();

            this.count = header.readableBytes() + payload.readableBytes();
        }

        @Override
        public long position() {
            long pos = 0;
            for (ByteBuffer buffer : this.buffers) {
                pos += buffer.position();
            }
            return pos;
        }

        @Override
        public long transfered() {
            return transferred;
        }

        @Override
        public long count() {
            return count;
        }

        @Override
        public long transferTo(WritableByteChannel target, long position) throws IOException {
            GatheringByteChannel channel = (GatheringByteChannel) target;
            long write = channel.write(this.buffers);
            transferred += write;
            return write;
        }

        @Override
        public int refCnt() {
            return 0;
        }

        @Override
        public ReferenceCounted retain() {
            return null;
        }

        @Override
        public ReferenceCounted retain(int increment) {
            return null;
        }

        @Override
        public boolean release() {
            header.release();
            return payload.release();
        }

        @Override
        public boolean release(int decrement) {
            header.release();
            return payload.release(decrement);
        }
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
            this.subject = pullRequest.getPartitionName();
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
            final ByteBuf payload = toPayload(pullMessageResult, requestHeader);
            ctx.writeAndFlush(new DataTransfer(buildResponseHeader(CommandCode.SUCCESS, requestHeader), payload)).addListener(future -> monitorPullProcessTime());
        }

        private void monitorPullProcessTime() {
            QMon.pullProcessTime(subject, group, System.currentTimeMillis() - pullBegin);
        }

        private ByteBuf toPayload(final PullMessageResult result, final RemotingHeader requestHeader) {
            final long start = System.currentTimeMillis();
            try {
                int payloadSize = 8 + 8 + result.getBufferTotalSize();
                final ByteBuf output = ByteBufAllocator.DEFAULT.ioBuffer(payloadSize);
                output.writeLong(result.getPullLogOffset());
                output.writeLong(-1);

                final List<Buffer> buffers = result.getBuffers();
                for (final Buffer buffer : buffers) {
                    ByteBuffer message = buffer.getBuffer();
                    //新客户端拉取消息
                    if (requestHeader.getVersion() >= VERSION_8) {
                        output.writeBytes(message);
                    } else {
                        //老客户端拉取消息
                        message.mark();
                        byte flag = message.get();
                        //老客户端拉取消息，但是没有tag
                        if (!Flags.hasTags(flag)) {
                            message.reset();
                            output.writeBytes(message);
                        } else {
                            //老客户端拉取有tag的消息
                            removeTags(output, message);
                        }
                    }
                }
                return output;
            } finally {
                release(result);
                QMon.readPullResultAsBytesElapsed(subject, group, System.currentTimeMillis() - start);
            }
        }

        private void release(PullMessageResult result) {
            List<Buffer> buffers = result.getBuffers();
            for (Buffer buffer : buffers) {
                buffer.release();
            }
        }

        private void removeTags(ByteBuf payloadBuffer, ByteBuffer message) {
            skip(message, 8 + 8);
            short subjectLen = message.getShort();
            skip(message, subjectLen);

            short messageIdLen = message.getShort();
            skip(message, messageIdLen);

            int current = message.position();
            message.reset();
            int originalLimit = message.limit();
            message.limit(current);
            payloadBuffer.writeBytes(message);

            message.limit(originalLimit);
            message.position(current);

            skipTags(message);
            payloadBuffer.writeBytes(message);
        }

        private void skipTags(ByteBuffer message) {
            byte tagSize = message.get();
            for (int i = 0; i < tagSize; i++) {
                short tagLen = message.getShort();
                skip(message, tagLen);
            }
        }

        private void skip(ByteBuffer buffer, int bytes) {
            buffer.position(buffer.position() + bytes);
        }

    }
}