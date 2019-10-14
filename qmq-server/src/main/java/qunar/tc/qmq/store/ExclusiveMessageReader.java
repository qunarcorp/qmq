package qunar.tc.qmq.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.ConsumerSequence;
import qunar.tc.qmq.base.PullMessageResult;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.consumer.ConsumerSequenceManager;
import qunar.tc.qmq.monitor.QMon;
import qunar.tc.qmq.order.ExclusiveConsumerLockManager;
import qunar.tc.qmq.protocol.consumer.PullRequest;
import qunar.tc.qmq.utils.RetryPartitionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * 用于独占模式的读取
 * 独占模式即某个时刻，一个consumer独占的消费一个partition上的消息
 * 独占模式的特点就是不需要单独为每个consumer维护拉取状态了，对于一个consumerGroup维护一个状态即可，所以也就不需要pull log了
 */
public class ExclusiveMessageReader extends MessageReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExclusiveMessageReader.class);

    private final Storage storage;
    private final ConsumerSequenceManager consumerSequenceManager;
    private final ExclusiveConsumerLockManager lockManager;

    public ExclusiveMessageReader(Storage storage, ConsumerSequenceManager consumerSequenceManager, ExclusiveConsumerLockManager lockManager, DynamicConfig config) {
        super(config);
        this.storage = storage;
        this.consumerSequenceManager = consumerSequenceManager;
        this.lockManager = lockManager;
    }

    /**
     * 独占消费时，会根据consumer拉取请求里的offset进行拉取，所以consumer可以决定从何位置开始拉取
     * 如果consumer没有指定拉取位置，也就是-1，那么就使用consumer的ack位置，如果ack位置也是-1，也就是从来没有消费过
     * 那么就从0开始拉取
     *
     * @param pullRequest 拉取请求
     * @return 拉取到的结果
     */
    @Override
    public PullMessageResult findMessages(PullRequest pullRequest) {
        String partitionName = pullRequest.getPartitionName();
        String consumerGroup = pullRequest.getGroup();
        String consumerId = pullRequest.getConsumerId();

        //独占消费时候有个独占锁，只有获取这个锁之后才允许读取消息，防止有多个消费者同时拉取同一个partition
        if (!lockManager.acquireLock(partitionName, consumerGroup, consumerId)) {
            // 获取锁失败
            return PullMessageResult.ACQUIRE_LOCK_FAILED;
        }

        final ConsumerSequence consumerSequence = consumerSequenceManager.getOrCreateConsumerSequence(partitionName, consumerGroup, consumerId, true);

        final long ackSequence = consumerSequence.getAckSequence();
        long pullSequenceFromConsumer = pullRequest.getPullOffsetLast();

        //如果消费者拉取请求的位置小于0，则使用server端保存的ack位置
        pullSequenceFromConsumer = pullSequenceFromConsumer < 0 ? ackSequence : pullSequenceFromConsumer;

        /*
         * pullSequenceFromConsumer表示的是上一次consumer拉取到的最后一条消息的位置，那么当前这次拉取我们其实是要拉取下一条消息
         * 所以需要将这个位置递增1
         *
         * @FIXME(zhaohui.yu)
         * 但是这里存在一个问题，如果consumer端已经拉到了10条消息，那么consumer拿到的位置是[0,9]，然后消费端因某种原因,
         * 希望将第10条，也就是[9]这个位置的消息重新消费，那么用户将拉取位置设置为9，则实际上拿到的是第11条消息，位置是[10]的消息。
         * 不过目前我们并没有提供给用户设置从某个位置拉取的API，当需要提供这种API的时候需要注意如何处理
         */
        pullSequenceFromConsumer = pullSequenceFromConsumer + 1;

        final long start = System.currentTimeMillis();
        try {
            GetMessageResult result = pollMessages(partitionName, pullSequenceFromConsumer, pullRequest.getRequestNum());
            switch (result.getStatus()) {
                case SUCCESS:
                    long actualSequence = result.getNextBeginSequence() - result.getMessageNum();
                    if (result.getMessageNum() == 0) {
                        return PullMessageResult.EMPTY;
                    }
                    confirmExpiredMessages(partitionName, consumerGroup, pullSequenceFromConsumer, actualSequence, result);
                    if (noPullFilter(pullRequest)) {
                        return new PullMessageResult(actualSequence, result.getBuffers(), result.getBufferTotalSize(), result.getMessageNum());
                    }

                    return doPullResultFilter(pullRequest, result);
                case OFFSET_OVERFLOW:
                    LOGGER.warn("get message result not success, consumer:{}, result:{}", pullRequest, result);
                    QMon.getMessageOverflowCountInc(partitionName, consumerGroup);
                default:
                    return PullMessageResult.EMPTY;
            }
        } catch (Throwable e) {
            LOGGER.error("find messages error, consumer: {}", pullRequest, e);
            QMon.findMessagesErrorCountInc(partitionName, consumerGroup);
        } finally {
            QMon.findNewExistMessageTime(partitionName, consumerGroup, System.currentTimeMillis() - start);
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
