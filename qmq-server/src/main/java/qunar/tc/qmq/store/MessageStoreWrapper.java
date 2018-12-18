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

package qunar.tc.qmq.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.TagType;
import qunar.tc.qmq.base.*;
import qunar.tc.qmq.consumer.ConsumerSequenceManager;
import qunar.tc.qmq.monitor.QMon;
import qunar.tc.qmq.protocol.consumer.PullRequest;
import qunar.tc.qmq.protocol.producer.MessageProducerCode;
import qunar.tc.qmq.store.action.RangeAckAction;

import java.util.ArrayList;
import java.util.List;

import static qunar.tc.qmq.store.Tags.match;

/**
 * @author yunfeng.yang
 * @since 2017/7/10
 */
public class MessageStoreWrapper {
    private static final Logger LOG = LoggerFactory.getLogger(MessageStoreWrapper.class);

    private final Storage storage;
    private final ConsumerSequenceManager consumerSequenceManager;

    public MessageStoreWrapper(final Storage storage, final ConsumerSequenceManager consumerSequenceManager) {
        this.storage = storage;
        this.consumerSequenceManager = consumerSequenceManager;
    }

    public ReceiveResult putMessage(final ReceivingMessage message) {
        final RawMessage rawMessage = message.getMessage();
        final MessageHeader header = rawMessage.getHeader();
        final String msgId = header.getMessageId();
        final long start = System.currentTimeMillis();
        try {
            final PutMessageResult putMessageResult = storage.appendMessage(rawMessage);
            final PutMessageStatus status = putMessageResult.getStatus();
            if (status != PutMessageStatus.SUCCESS) {
                LOG.error("put message error, message:{} {}, status:{}", header.getSubject(), msgId, status.name());
                QMon.storeMessageErrorCountInc(header.getSubject());
                return new ReceiveResult(msgId, MessageProducerCode.STORE_ERROR, status.name(), -1);
            }

            AppendMessageResult<MessageSequence> result = putMessageResult.getResult();
            final long endOffsetOfMessage = result.getWroteOffset() + result.getWroteBytes();
            return new ReceiveResult(msgId, MessageProducerCode.SUCCESS, "", endOffsetOfMessage);
        } catch (Throwable e) {
            LOG.error("put message error, message:{} {}", header.getSubject(), header.getMessageId(), e);
            QMon.storeMessageErrorCountInc(header.getSubject());
            return new ReceiveResult(msgId, MessageProducerCode.STORE_ERROR, "", -1);
        } finally {
            QMon.putMessageTime(header.getSubject(), System.currentTimeMillis() - start);
        }
    }

    public PullMessageResult findMessages(final PullRequest pullRequest) {
        try {
            final PullMessageResult unAckMessages = findUnAckMessages(pullRequest);
            if (unAckMessages.getMessageNum() > 0) {
                return unAckMessages;
            }

            return findNewExistMessages(pullRequest);
        } catch (Throwable e) {
            LOG.error("find messages error, consumer: {}", pullRequest, e);
            QMon.findMessagesErrorCountInc(pullRequest.getSubject(), pullRequest.getGroup());
        }
        return PullMessageResult.EMPTY;
    }

    private PullMessageResult findNewExistMessages(final PullRequest pullRequest) {
        final String subject = pullRequest.getSubject();
        final String group = pullRequest.getGroup();
        final String consumerId = pullRequest.getConsumerId();
        final boolean isBroadcast = pullRequest.isBroadcast();

        final long start = System.currentTimeMillis();
        try {
            ConsumeQueue consumeQueue = storage.locateConsumeQueue(subject, group);
            final GetMessageResult getMessageResult = consumeQueue.pollMessages(pullRequest.getRequestNum());
            switch (getMessageResult.getStatus()) {
                case SUCCESS:
                    if (getMessageResult.getMessageNum() == 0) {
                        consumeQueue.setNextSequence(getMessageResult.getNextBeginOffset());
                        return PullMessageResult.EMPTY;
                    }

                    if (noRequestTag(pullRequest)) {
                        final WritePutActionResult writeResult = consumerSequenceManager.putPullActions(subject, group, consumerId, isBroadcast, getMessageResult);
                        if (writeResult.isSuccess()) {
                            consumeQueue.setNextSequence(getMessageResult.getNextBeginOffset());
                            return new PullMessageResult(writeResult.getPullLogOffset(), getMessageResult.getSegmentBuffers(), getMessageResult.getBufferTotalSize(), getMessageResult.getMessageNum());
                        } else {
                            getMessageResult.release();
                            return PullMessageResult.EMPTY;
                        }
                    }

                    return filterByTags(pullRequest, getMessageResult, consumeQueue);
                case OFFSET_OVERFLOW:
                    LOG.warn("get message result not success, consumer:{}, result:{}", pullRequest, getMessageResult);
                    QMon.getMessageOverflowCountInc(subject, group);
                default:
                    consumeQueue.setNextSequence(getMessageResult.getNextBeginOffset());
                    return PullMessageResult.EMPTY;
            }
        } finally {
            QMon.findNewExistMessageTime(subject, group, System.currentTimeMillis() - start);
        }
    }

    private boolean noRequestTag(PullRequest pullRequest) {
        int tagTypeCode = pullRequest.getTagTypeCode();
        if (TagType.NO_TAG.getCode() == tagTypeCode) return true;
        List<byte[]> tags = pullRequest.getTags();
        return tags == null || tags.isEmpty();
    }

    private PullMessageResult filterByTags(PullRequest pullRequest, GetMessageResult getMessageResult, ConsumeQueue consumeQueue) {
        final String subject = pullRequest.getSubject();
        final String group = pullRequest.getGroup();
        final String consumerId = pullRequest.getConsumerId();
        final boolean isBroadcast = pullRequest.isBroadcast();

        shiftRight(getMessageResult);
        List<GetMessageResult> filterResult = filter(getMessageResult, pullRequest.getTags(), pullRequest.getTagTypeCode());
        List<PullMessageResult> retList = new ArrayList<>();
        int index;
        for (index = 0; index < filterResult.size(); ++index) {
            GetMessageResult item = filterResult.get(index);
            if (!putAction(item, consumeQueue, subject, group, consumerId, isBroadcast, retList)) break;
        }
        releaseRemain(index, filterResult);
        if (retList.isEmpty()) return PullMessageResult.FILTER_EMPTY;
        return merge(retList);
    }

    private List<GetMessageResult> filter(GetMessageResult input, List<byte[]> tags, int tagType) {
        List<GetMessageResult> result = new ArrayList<>();

        List<SegmentBuffer> messages = input.getSegmentBuffers();
        OffsetRange offsetRange = input.getConsumerLogRange();

        GetMessageResult range = null;
        long begin = -1;
        long end = -1;
        for (int i = 0; i < messages.size(); ++i) {
            SegmentBuffer message = messages.get(i);
            if (match(message, tags, tagType)) {
                if (range == null) {
                    range = new GetMessageResult();
                    result.add(range);
                    begin = offsetRange.getBegin() + i;
                }
                end = offsetRange.getBegin() + i;
                range.addSegmentBuffer(message);
            } else {
                message.release();
                setOffsetRange(range, begin, end);
                range = null;
            }
        }
        setOffsetRange(range, begin, end);
        appendEmpty(end, offsetRange, result);
        return result;
    }

    private void setOffsetRange(GetMessageResult input, long begin, long end) {
        if (input != null) {
            input.setConsumerLogRange(new OffsetRange(begin, end));
            input.setNextBeginOffset(end + 1);
        }
    }

    /*
    begin=0                           end=8
    ------------------------------------
    | - | - | - | + | + | + | + | + | + |
    -------------------------------------
    shift -> begin=3, end=8
     */
    private void shiftRight(GetMessageResult getMessageResult) {
        OffsetRange offsetRange = getMessageResult.getConsumerLogRange();
        long expectedBegin = offsetRange.getEnd() - getMessageResult.getMessageNum() + 1;
        if (expectedBegin == offsetRange.getBegin()) return;
        getMessageResult.setConsumerLogRange(new OffsetRange(expectedBegin, offsetRange.getEnd()));
    }

    private boolean putAction(GetMessageResult range, ConsumeQueue consumeQueue,
                              String subject, String group, String consumerId, boolean isBroadcast,
                              List<PullMessageResult> retList) {
        final WritePutActionResult writeResult = consumerSequenceManager.putPullActions(subject, group, consumerId, isBroadcast, range);
        if (writeResult.isSuccess()) {
            consumeQueue.setNextSequence(range.getNextBeginOffset());
            retList.add(new PullMessageResult(writeResult.getPullLogOffset(), range.getSegmentBuffers(), range.getBufferTotalSize(), range.getMessageNum()));
            return true;
        }
        return false;
    }

    private PullMessageResult merge(List<PullMessageResult> list) {
        if (list.size() == 1) return list.get(0);

        long pullLogOffset = list.get(0).getPullLogOffset();
        List<SegmentBuffer> buffers = new ArrayList<>();
        int bufferTotalSize = 0;
        int messageNum = 0;
        for (PullMessageResult result : list) {
            bufferTotalSize += result.getBufferTotalSize();
            messageNum += result.getMessageNum();
            buffers.addAll(result.getBuffers());
        }
        return new PullMessageResult(pullLogOffset, buffers, bufferTotalSize, messageNum);
    }

    private void appendEmpty(long end, OffsetRange offsetRange, List<GetMessageResult> list) {
        if (end < offsetRange.getEnd()) {
            GetMessageResult emptyRange = new GetMessageResult();
            long begin = end == -1 ? offsetRange.getBegin() : end;
            emptyRange.setConsumerLogRange(new OffsetRange(begin, offsetRange.getEnd()));
            emptyRange.setNextBeginOffset(offsetRange.getEnd() + 1);
            list.add(emptyRange);
        }
    }

    private void releaseRemain(int startIndex, List<GetMessageResult> list) {
        for (int i = startIndex; i < list.size(); ++i) {
            list.get(i).release();
        }
    }

    private PullMessageResult findUnAckMessages(final PullRequest pullRequest) {
        final long start = System.currentTimeMillis();
        try {
            return doFindUnAckMessages(pullRequest);
        } finally {
            QMon.findLostMessagesTime(pullRequest.getSubject(), pullRequest.getGroup(), System.currentTimeMillis() - start);
        }
    }

    private PullMessageResult doFindUnAckMessages(final PullRequest pullRequest) {
        final String subject = pullRequest.getSubject();
        final String group = pullRequest.getGroup();
        final String consumerId = pullRequest.getConsumerId();
        final ConsumerSequence consumerSequence = consumerSequenceManager.getOrCreateConsumerSequence(subject, group, consumerId);

        final long ackSequence = consumerSequence.getAckSequence();
        long pullLogSequenceInConsumer = pullRequest.getPullOffsetLast();
        if (pullLogSequenceInConsumer < ackSequence) {
            pullLogSequenceInConsumer = ackSequence;
        }

        final long pullLogSequenceInServer = consumerSequence.getPullSequence();
        if (pullLogSequenceInServer <= pullLogSequenceInConsumer) {
            return PullMessageResult.EMPTY;
        }

        LOG.warn("consumer need find lost ack messages, pullRequest: {}, consumerSequence: {}", pullRequest, consumerSequence);

        final int requestNum = pullRequest.getRequestNum();
        final List<SegmentBuffer> buffers = new ArrayList<>(requestNum);
        long firstValidSeq = -1;
        int totalSize = 0;
        final long firstLostAckPullLogSeq = pullLogSequenceInConsumer + 1;
        for (long seq = firstLostAckPullLogSeq; buffers.size() < requestNum && seq <= pullLogSequenceInServer; seq++) {
            try {
                final long consumerLogSequence = getConsumerLogSequence(pullRequest, seq);
                //deleted message
                if (consumerLogSequence < 0) {
                    LOG.warn("find no consumer log for this pull log sequence, req: {}, pullLogSeq: {}, consumerLogSeq: {}", pullRequest, seq, consumerLogSequence);
                    if (firstValidSeq == -1) {
                        continue;
                    } else {
                        break;
                    }
                }

                final GetMessageResult getMessageResult = storage.getMessage(subject, consumerLogSequence);
                if (getMessageResult.getStatus() != GetMessageStatus.SUCCESS || getMessageResult.getMessageNum() == 0) {
                    LOG.error("getMessageResult error, consumer:{}, consumerLogSequence:{}, pullLogSequence:{}, getMessageResult:{}", pullRequest, consumerLogSequence, seq, getMessageResult);
                    QMon.getMessageErrorCountInc(subject, group);
                    if (firstValidSeq == -1) {
                        continue;
                    } else {
                        break;
                    }
                }

                //re-filter un-ack message
                final SegmentBuffer segmentBuffer = getMessageResult.getSegmentBuffers().get(0);
                if (!noRequestTag(pullRequest) && !match(segmentBuffer, pullRequest.getTags(), pullRequest.getTagTypeCode())) {
                    if (firstValidSeq != -1) {
                        break;
                    }
                }

                if (firstValidSeq == -1) {
                    firstValidSeq = seq;
                }


                buffers.add(segmentBuffer);
                totalSize += segmentBuffer.getSize();
            } catch (Exception e) {
                LOG.error("error occurs when find messages by pull log offset, request: {}, consumerSequence: {}", pullRequest, consumerSequence, e);
                QMon.getMessageErrorCountInc(subject, group);

                if (firstValidSeq != -1) {
                    break;
                }
            }
        }

        if (buffers.size() > 0) {
            //说明pull log里有一段对应的消息已经被清理掉了，需要调整一下位置
            if (firstValidSeq > firstLostAckPullLogSeq) {
                consumerSequence.setAckSequence(firstValidSeq - 1);
            }
        } else {
            LOG.error("find lost messages empty, consumer:{}, consumerSequence:{}, pullLogSequence:{}", pullRequest, consumerSequence, pullLogSequenceInServer);
            QMon.findLostMessageEmptyCountInc(subject, group);
            firstValidSeq = pullLogSequenceInServer;
            consumerSequence.setAckSequence(pullLogSequenceInServer);

            // auto ack all deleted pulled message
            LOG.info("auto ack deleted pulled message. subject: {}, group: {}, consumerId: {}, firstSeq: {}, lastSeq: {}",
                    subject, group, consumerId, firstLostAckPullLogSeq, firstValidSeq);
            consumerSequenceManager.putAction(new RangeAckAction(subject, group, consumerId, System.currentTimeMillis(), firstLostAckPullLogSeq, firstValidSeq));
        }

        final PullMessageResult result = new PullMessageResult(firstValidSeq, buffers, totalSize, buffers.size());
        QMon.findLostMessageCountInc(subject, group, result.getMessageNum());
        LOG.info("found lost ack messages request: {}, consumerSequence: {}, result: {}", pullRequest, consumerSequence, result);
        return result;
    }

    private long getConsumerLogSequence(PullRequest pullRequest, long offset) {
        if (pullRequest.isBroadcast()) return offset;
        return storage.getMessageSequenceByPullLog(pullRequest.getSubject(), pullRequest.getGroup(), pullRequest.getConsumerId(), offset);
    }

    public long getQueueCount(String subject, String group) {
        return storage.locateConsumeQueue(subject, group).getQueueCount();
    }
}
