package qunar.tc.qmq.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.ConsumerSequence;
import qunar.tc.qmq.base.PullMessageResult;
import qunar.tc.qmq.base.WritePutActionResult;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.consumer.ConsumerSequenceManager;
import qunar.tc.qmq.monitor.QMon;
import qunar.tc.qmq.protocol.consumer.PullRequest;
import qunar.tc.qmq.store.action.RangeAckAction;
import qunar.tc.qmq.store.buffer.Buffer;

import java.util.ArrayList;
import java.util.List;

/**
 * 用于共享模式的读取
 * 共享模式即多个consumer可以同时消费同一个partition上的消息，所以共享模式需要给每个consumer分别维护拉取状态
 */
public class SharedMessageReader extends MessageReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(SharedMessageReader.class);

    private static final int MAX_MEMORY_LIMIT = 100 * 1024 * 1024;

    private final Storage storage;
    private final ConsumerSequenceManager consumerSequenceManager;

    public SharedMessageReader(Storage storage, ConsumerSequenceManager consumerSequenceManager, DynamicConfig config) {
        super(config);
        this.storage = storage;
        this.consumerSequenceManager = consumerSequenceManager;
    }

    /**
     * 共享消费拉取的时候，首先要根据该consumer的ack位置以及pull log的位置，判断是否将已经拉取(也就是存在pull log里的)但是仍未ack的消息重新发送
     * 如果不存在未ack的消息，则会拉取新消息，但是拉取的位置以server端记录的为准，并不参考consumer传递上来的位置，因为共享消费时是多个consumer竞争
     * 消费相同的partition，单个consumer的拉取请求位置只表示自己的位置
     * @param pullRequest
     * @return
     */
    @Override
    public PullMessageResult findMessages(final PullRequest pullRequest) {
        try {
            final PullMessageResult unAckMessages = findUnAckMessages(pullRequest);
            if (unAckMessages.getMessageNum() > 0) {
                return unAckMessages;
            }

            return findNewExistMessages(pullRequest);
        } catch (Throwable e) {
            LOGGER.error("find messages error, consumer: {}", pullRequest, e);
            QMon.findMessagesErrorCountInc(pullRequest.getPartitionName(), pullRequest.getGroup());
        }
        return PullMessageResult.EMPTY;
    }

    private PullMessageResult findNewExistMessages(final PullRequest pullRequest) {
        final String subject = pullRequest.getPartitionName();
        final String consumerGroup = pullRequest.getGroup();
        final String consumerId = pullRequest.getConsumerId();

        final long start = System.currentTimeMillis();
        try {
            ConsumeQueue consumeQueue = storage.locateConsumeQueue(subject, consumerGroup);
            final GetMessageResult getMessageResult = consumeQueue.pollMessages(pullRequest.getRequestNum());
            switch (getMessageResult.getStatus()) {
                case SUCCESS:
                    if (getMessageResult.getMessageNum() == 0) {
                        consumeQueue.setNextSequence(getMessageResult.getNextBeginSequence());
                        return PullMessageResult.EMPTY;
                    }

                    if (noPullFilter(pullRequest)) {
                        final WritePutActionResult writeResult = consumerSequenceManager.putPullActions(subject, consumerGroup, consumerId, false, getMessageResult);
                        if (writeResult.isSuccess()) {
                            consumeQueue.setNextSequence(getMessageResult.getNextBeginSequence());
                            return new PullMessageResult(writeResult.getPullLogOffset(), getMessageResult.getBuffers(), getMessageResult.getBufferTotalSize(), getMessageResult.getMessageNum());
                        } else {
                            getMessageResult.release();
                            return PullMessageResult.EMPTY;
                        }
                    }

                    return doPullResultFilter(pullRequest, getMessageResult, consumeQueue);
                case OFFSET_OVERFLOW:
                    LOGGER.warn("get message result not success, consumer:{}, result:{}", pullRequest, getMessageResult);
                    QMon.getMessageOverflowCountInc(subject, consumerGroup);
                default:
                    consumeQueue.setNextSequence(getMessageResult.getNextBeginSequence());
                    return PullMessageResult.EMPTY;
            }
        } finally {
            QMon.findNewExistMessageTime(subject, consumerGroup, System.currentTimeMillis() - start);
        }
    }

    private PullMessageResult doPullResultFilter(PullRequest pullRequest, GetMessageResult getMessageResult, ConsumeQueue consumeQueue) {
        final String subject = pullRequest.getPartitionName();
        final String consumerGroup = pullRequest.getGroup();
        final String consumerId = pullRequest.getConsumerId();

        shiftRight(getMessageResult);
        List<GetMessageResult> filterResult = filter(pullRequest, getMessageResult);
        List<PullMessageResult> retList = new ArrayList<>();
        int index;
        for (index = 0; index < filterResult.size(); ++index) {
            GetMessageResult item = filterResult.get(index);
            if (!putAction(item, consumeQueue, subject, consumerGroup, consumerId, retList))
                break;
        }
        releaseRemain(index, filterResult);
        if (retList.isEmpty()) return PullMessageResult.FILTER_EMPTY;
        return merge(retList);
    }

    private void releaseRemain(int startIndex, List<GetMessageResult> list) {
        for (int i = startIndex; i < list.size(); ++i) {
            list.get(i).release();
        }
    }

    private boolean putAction(GetMessageResult range, ConsumeQueue consumeQueue,
                              String subject, String consumerGroup, String consumerId,
                              List<PullMessageResult> retList) {
        final WritePutActionResult writeResult = consumerSequenceManager.putPullActions(subject, consumerGroup, consumerId, false, range);
        if (writeResult.isSuccess()) {
            consumeQueue.setNextSequence(range.getNextBeginSequence());
            retList.add(new PullMessageResult(writeResult.getPullLogOffset(), range.getBuffers(), range.getBufferTotalSize(), range.getMessageNum()));
            return true;
        }
        return false;
    }

    private PullMessageResult findUnAckMessages(final PullRequest pullRequest) {
        final long start = System.currentTimeMillis();
        try {
            return doFindUnAckMessages(pullRequest);
        } finally {
            QMon.findLostMessagesTime(pullRequest.getPartitionName(), pullRequest.getGroup(), System.currentTimeMillis() - start);
        }
    }

    private PullMessageResult doFindUnAckMessages(final PullRequest pullRequest) {
        final String subject = pullRequest.getPartitionName();
        final String consumerGroup = pullRequest.getGroup();
        final String consumerId = pullRequest.getConsumerId();
        final ConsumerSequence consumerSequence = consumerSequenceManager.getOrCreateConsumerSequence(subject, consumerGroup, consumerId, false);

        final long ackSequence = consumerSequence.getAckSequence();
        long pullLogSequenceInConsumer = pullRequest.getPullOffsetLast();
        if (pullLogSequenceInConsumer < ackSequence) {
            pullLogSequenceInConsumer = ackSequence;
        }

        final long pullLogSequenceInServer = consumerSequence.getPullSequence();
        if (pullLogSequenceInServer <= pullLogSequenceInConsumer) {
            return PullMessageResult.EMPTY;
        }

        LOGGER.warn("consumer need find lost ack messages, pullRequest: {}, consumerSequence: {}", pullRequest, consumerSequence);

        final int requestNum = pullRequest.getRequestNum();
        final List<Buffer> buffers = new ArrayList<>(requestNum);
        long firstValidSeq = -1;
        int totalSize = 0;
        final long firstLostAckPullLogSeq = pullLogSequenceInConsumer + 1;
        for (long seq = firstLostAckPullLogSeq; buffers.size() < requestNum && seq <= pullLogSequenceInServer; seq++) {
            try {
                final long consumerLogSequence = getConsumerLogSequence(pullRequest, seq);
                //deleted message
                if (consumerLogSequence < 0) {
                    LOGGER.warn("find no consumer log for this pull log sequence, req: {}, pullLogSeq: {}, consumerLogSeq: {}", pullRequest, seq, consumerLogSequence);
                    if (firstValidSeq == -1) {
                        continue;
                    } else {
                        break;
                    }
                }

                final GetMessageResult getMessageResult = storage.getMessage(subject, consumerLogSequence);
                if (getMessageResult.getStatus() != GetMessageStatus.SUCCESS || getMessageResult.getMessageNum() == 0) {
                    LOGGER.error("getMessageResult error, consumer:{}, consumerLogSequence:{}, pullLogSequence:{}, getMessageResult:{}", pullRequest, consumerLogSequence, seq, getMessageResult);
                    QMon.getMessageErrorCountInc(subject, consumerGroup);
                    if (firstValidSeq == -1) {
                        continue;
                    } else {
                        break;
                    }
                }

                //re-filter un-ack message
                //避免客户端发布一个新版本，新版本里tags发生变化了，那么原来已经拉取但是未ack的消息就可能存在不符合新条件的消息了
                //这个时候需要重新过滤一遍
                final Buffer segmentBuffer = getMessageResult.getBuffers().get(0);
                if (!noPullFilter(pullRequest) && !needKeep(pullRequest, segmentBuffer)) {
                    segmentBuffer.release();
                    if (firstValidSeq != -1) {
                        break;
                    } else {
                        continue;
                    }
                }

                if (firstValidSeq == -1) {
                    firstValidSeq = seq;
                }

                buffers.add(segmentBuffer);
                totalSize += segmentBuffer.getSize();

                //超过一次读取的内存限制
                if (totalSize >= MAX_MEMORY_LIMIT) break;
            } catch (Exception e) {
                LOGGER.error("error occurs when find messages by pull log offset, request: {}, consumerSequence: {}", pullRequest, consumerSequence, e);
                QMon.getMessageErrorCountInc(subject, consumerGroup);

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
            LOGGER.error("find lost messages empty, consumer:{}, consumerSequence:{}, pullLogSequence:{}", pullRequest, consumerSequence, pullLogSequenceInServer);
            QMon.findLostMessageEmptyCountInc(subject, consumerGroup);
            firstValidSeq = pullLogSequenceInServer;
            consumerSequence.setAckSequence(pullLogSequenceInServer);

            // auto ack all deleted pulled message
            LOGGER.info("auto ack deleted pulled message. subject: {}, consumerGroup: {}, consumerId: {}, firstSeq: {}, lastSeq: {}",
                    subject, consumerGroup, consumerId, firstLostAckPullLogSeq, firstValidSeq);
            consumerSequenceManager.putAction(new RangeAckAction(subject, consumerGroup, consumerId, System.currentTimeMillis(), firstLostAckPullLogSeq, firstValidSeq));
        }

        final PullMessageResult result = new PullMessageResult(firstValidSeq, buffers, totalSize, buffers.size());
        QMon.findLostMessageCountInc(subject, consumerGroup, result.getMessageNum());
        LOGGER.info("found lost ack messages request: {}, consumerSequence: {}, result: {}", pullRequest, consumerSequence, result);
        return result;
    }

    private long getConsumerLogSequence(PullRequest pullRequest, long offset) {
        return storage.getMessageSequenceByPullLog(pullRequest.getPartitionName(), pullRequest.getGroup(), pullRequest.getConsumerId(), offset);
    }
}
