package qunar.tc.qmq.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.ConsumerSequence;
import qunar.tc.qmq.base.PullMessageResult;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.consumer.ConsumerSequenceManager;
import qunar.tc.qmq.monitor.QMon;
import qunar.tc.qmq.order.ExclusiveMessageLockManager;
import qunar.tc.qmq.protocol.consumer.PullRequest;
import qunar.tc.qmq.utils.RetryPartitionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * 用于独占模式的读取
 * 独占模式即某个时刻，一个consumer独占的消费一个partition上的消息
 * 独占模式的特点就是不需要为consumer维护拉取状态了，所以对于一个consumerGroup维护一个状态即可，所以也就不需要pull log了
 */
public class ExclusiveMessageReader extends MessageReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExclusiveMessageReader.class);

    private final Storage storage;
    private final ConsumerSequenceManager consumerSequenceManager;
    private final ExclusiveMessageLockManager lockManager;

    public ExclusiveMessageReader(Storage storage, ConsumerSequenceManager consumerSequenceManager, ExclusiveMessageLockManager lockManager, DynamicConfig config) {
        super(config);
        this.storage = storage;
        this.consumerSequenceManager = consumerSequenceManager;
        this.lockManager = lockManager;
    }

    @Override
    public PullMessageResult findMessages(PullRequest pullRequest) {
        String subject = pullRequest.getPartitionName();
        String consumerGroup = pullRequest.getGroup();
        String consumerId = pullRequest.getConsumerId();

        if (!lockManager.acquireLock(subject, consumerGroup, consumerId)) {
            // 获取锁失败
            return PullMessageResult.REJECT;
        }

        final ConsumerSequence consumerSequence = consumerSequenceManager.getOrCreateConsumerSequence(subject, consumerGroup, consumerId, true);

        final long ackSequence = consumerSequence.getAckSequence();
        long pullLogSequenceInConsumer = pullRequest.getPullOffsetLast();
        if (pullLogSequenceInConsumer < ackSequence) {
            pullLogSequenceInConsumer = ackSequence;
        }

        final long start = System.currentTimeMillis();
        try {
            GetMessageResult result = pollMessages(subject, pullLogSequenceInConsumer, pullRequest.getRequestNum());
            switch (result.getStatus()) {
                case SUCCESS:
                    long actualSequence = result.getNextBeginSequence() - result.getMessageNum();
                    if (result.getMessageNum() == 0) {
                        return PullMessageResult.EMPTY;
                    }
                    confirmExpiredMessages(subject, consumerGroup, pullLogSequenceInConsumer, actualSequence, result);
                    if (noPullFilter(pullRequest)) {
                        return new PullMessageResult(actualSequence, result.getBuffers(), result.getBufferTotalSize(), result.getMessageNum());
                    }

                    return doPullResultFilter(pullRequest, result);
                case OFFSET_OVERFLOW:
                    LOGGER.warn("get message result not success, consumer:{}, result:{}", pullRequest, result);
                    QMon.getMessageOverflowCountInc(subject, consumerGroup);
                default:
                    return PullMessageResult.EMPTY;
            }
        } catch (Throwable e) {
            LOGGER.error("find messages error, consumer: {}", pullRequest, e);
            QMon.findMessagesErrorCountInc(subject, consumerGroup);
        } finally {
            QMon.findNewExistMessageTime(subject, consumerGroup, System.currentTimeMillis() - start);
        }
        return PullMessageResult.EMPTY;
    }

    private GetMessageResult pollMessages(String subject, long sequence, int maxMessages) {
        if (RetryPartitionUtils.isRetryPartitionName(subject)) {
            return storage.pollMessages(subject, sequence, maxMessages, this::isDelayReached);
        } else {
            return storage.pollMessages(subject, sequence, maxMessages);
        }
    }

    private boolean isDelayReached(MessageFilter.WithTimestamp entry) {
        final int delayMillis = storage.getStorageConfig().getRetryDelaySeconds() * 1000;
        return entry.getTimestamp() + delayMillis <= System.currentTimeMillis();
    }

    private PullMessageResult doPullResultFilter(PullRequest pullRequest, GetMessageResult getMessageResult) {
        shiftRight(getMessageResult);
        List<GetMessageResult> filterResult = filter(pullRequest, getMessageResult);
        List<PullMessageResult> retList = new ArrayList<>();
        for (GetMessageResult result : filterResult) {
            long begin = result.getNextBeginSequence() - result.getMessageNum();
            retList.add(new PullMessageResult(begin, result.getBuffers(), result.getBufferTotalSize(), result.getMessageNum()));
        }
        if (retList.isEmpty()) return PullMessageResult.FILTER_EMPTY;
        return merge(retList);
    }

    /**
     * requestSequence是期望开始拉取的位置，而actualSequence是从Storage拉取出的消息实际的开始位置，如果actualSequence比requestSequence大
     * 那么只能说明，这段消息已经过期被删除了，导致无法拉取到。
     *
     * @param subject         消息主题
     * @param consumerGroup   消费组
     * @param requestSequence 期望开始拉取的位置
     * @param actualSequence  实际开始拉取的位置
     * @param result          拉取到的消息集合
     */
    private void confirmExpiredMessages(String subject, String consumerGroup, long requestSequence, long actualSequence, GetMessageResult result) {
        long delta = actualSequence - requestSequence;
        if (delta > 0) {
            QMon.expiredMessagesCountInc(subject, consumerGroup, delta);
            LOGGER.error("next sequence skipped. subject: {}, group: {}, nextSequence: {}, result: {}", subject, consumerGroup, requestSequence, result);
        }
    }
}
