package qunar.tc.qmq.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.ConsumerSequence;
import qunar.tc.qmq.base.PullMessageResult;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.consumer.ConsumerSequenceManager;
import qunar.tc.qmq.monitor.QMon;
import qunar.tc.qmq.protocol.consumer.PullRequest;
import qunar.tc.qmq.utils.RetryPartitionUtils;

import java.util.ArrayList;
import java.util.List;

public class ExclusiveMessageReader extends MessageReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExclusiveMessageReader.class);

    private final Storage storage;
    private final ConsumerSequenceManager consumerSequenceManager;

    public ExclusiveMessageReader(Storage storage, ConsumerSequenceManager consumerSequenceManager, DynamicConfig config) {
        super(config);
        this.storage = storage;
        this.consumerSequenceManager = consumerSequenceManager;
    }

    @Override
    public PullMessageResult findMessages(PullRequest pullRequest) {
        final String subject = pullRequest.getPartitionName();
        final String consumerGroup = pullRequest.getGroup();
        String consumerId = pullRequest.getConsumerId();
        final ConsumerSequence consumerSequence = consumerSequenceManager.getOrCreateConsumerSequence(subject, consumerGroup, consumerId, true);

        final long ackSequence = consumerSequence.getAckSequence();
        long pullLogSequenceInConsumer = pullRequest.getPullOffsetLast();
        if (pullLogSequenceInConsumer < ackSequence) {
            pullLogSequenceInConsumer = ackSequence;
        }

        final long start = System.currentTimeMillis();
        try {
            GetMessageResult getMessageResult = pollMessages(subject, pullLogSequenceInConsumer, pullRequest.getRequestNum());
            switch (getMessageResult.getStatus()) {
                case SUCCESS:
                    long actualSequence = getMessageResult.getNextBeginSequence() - getMessageResult.getBuffers().size();
                    long delta = actualSequence - pullLogSequenceInConsumer;
                    if (delta > 0) {
                        QMon.expiredMessagesCountInc(subject, consumerGroup, delta);
                        LOGGER.error("next sequence skipped. subject: {}, group: {}, nextSequence: {}, result: {}", subject, consumerGroup, pullLogSequenceInConsumer, getMessageResult);
                    }
                    if (getMessageResult.getMessageNum() == 0) {
                        return PullMessageResult.EMPTY;
                    }

                    if (noPullFilter(pullRequest)) {
                        long begin = getMessageResult.getConsumerLogRange().getEnd() - getMessageResult.getMessageNum() + 1;
                        return new PullMessageResult(begin, getMessageResult.getBuffers(), getMessageResult.getBufferTotalSize(), getMessageResult.getMessageNum());
                    }

                    return doPullResultFilter(pullRequest, getMessageResult);
                case OFFSET_OVERFLOW:
                    LOGGER.warn("get message result not success, consumer:{}, result:{}", pullRequest, getMessageResult);
                    QMon.getMessageOverflowCountInc(subject, consumerGroup);
                default:
                    return PullMessageResult.EMPTY;
            }
        } catch (Throwable e) {
            LOGGER.error("find messages error, consumer: {}", pullRequest, e);
            QMon.findMessagesErrorCountInc(pullRequest.getPartitionName(), pullRequest.getGroup());
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
            long begin = result.getConsumerLogRange().getEnd() - getMessageResult.getMessageNum() + 1;
            retList.add(new PullMessageResult(begin, result.getBuffers(), result.getBufferTotalSize(), result.getMessageNum()));
        }
        if (retList.isEmpty()) return PullMessageResult.FILTER_EMPTY;
        return merge(retList);
    }
}
