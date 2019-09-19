package qunar.tc.qmq.producer.sender;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.MessageGroup;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.batch.MessageProcessor;
import qunar.tc.qmq.batch.OrderedSendMessageExecutor;
import qunar.tc.qmq.batch.SendMessageExecutor;
import qunar.tc.qmq.common.OrderStrategy;
import qunar.tc.qmq.common.OrderStrategyCache;
import qunar.tc.qmq.common.OrderStrategyManager;
import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.metrics.MetricsConstants;
import qunar.tc.qmq.metrics.QmqTimer;
import qunar.tc.qmq.producer.ConfigCenter;
import qunar.tc.qmq.producer.QueueSender;
import qunar.tc.qmq.service.exceptions.BlockMessageException;
import qunar.tc.qmq.service.exceptions.DuplicateMessageException;
import qunar.tc.qmq.service.exceptions.MessageException;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * 消息发送队列管理器, 每个队列由一个 MessageGroup 代表
 *
 * @author zhenwei.liu
 * @since 2019-08-20
 */
public class OrderedQueueSender implements QueueSender, MessageProcessor {
    private static final Logger logger = LoggerFactory.getLogger(OrderedQueueSender.class);

    private static final String SEND_MESSAGE_THROWABLE_COUNTER = "qmq_client_producer_send_message_Throwable";

    private final ExecutorService executor; // 所有分区共享一个线程池

    private final int maxQueueSize;
    private final int sendBatch;

    private final QmqTimer timer;
    private final ConnectionManager connectionManager;
    private final MessageGroupResolver messageGroupResolver;

    // subject-physicalPartition => executor
    private Map<MessageGroup, OrderedSendMessageExecutor> executorMap = Maps.newConcurrentMap();

    public OrderedQueueSender(ConnectionManager connectionManager, MessageGroupResolver messageGroupResolver) {
        this.connectionManager = connectionManager;
        this.messageGroupResolver = messageGroupResolver;

        ConfigCenter configs = ConfigCenter.getInstance();

        this.timer = Metrics.timer("qmq_client_producer_send_broker_time");
        this.maxQueueSize = configs.getMaxQueueSize();
        this.sendBatch = configs.getSendBatch();
        // TODO(zhenwei.liu) executor 并发度设计
        this.executor = Executors.newCachedThreadPool(new NamedThreadFactory("batch-ordered-qmq-sender-task", true));
    }

    // TODO(zhenwei.liu) 落库成功, 入队失败的消息会乱序
    @Override
    public boolean offer(ProduceMessage pm) {
        return getExecutor(pm).addMessage(pm);
    }

    @Override
    public boolean offer(ProduceMessage pm, MessageGroup messageGroup) {
        return getExecutor(messageGroup).addMessage(pm);
    }

    @Override
    public boolean offer(ProduceMessage pm, long millisecondWait) {
        return getExecutor(pm).addMessage(pm, millisecondWait);
    }

    @Override
    public void send(ProduceMessage pm) {
        OrderedSendMessageExecutor executor = getExecutor(pm);
        process(Collections.singletonList(pm), executor);
    }

    @Override
    public void send(ProduceMessage pm, MessageGroup messageGroup) {
        OrderedSendMessageExecutor executor = getExecutor(messageGroup);
        process(Collections.singletonList(pm), executor);
    }

    @Override
    public void destroy() {
        this.executor.shutdown();
    }

    private OrderedSendMessageExecutor getExecutor(ProduceMessage produceMessage) {
        BaseMessage baseMessage = (BaseMessage) produceMessage.getBase();
        MessageGroup messageGroup = messageGroupResolver.resolveGroup(baseMessage);
        produceMessage.setMessageGroup(messageGroup);
        return getExecutor(messageGroup);
    }

    private OrderedSendMessageExecutor getExecutor(MessageGroup messageGroup) {
        return executorMap.computeIfAbsent(messageGroup, group -> new OrderedSendMessageExecutor(messageGroup, sendBatch, maxQueueSize, this, executor));
    }

    @Override
    public void process(List<ProduceMessage> produceMessages, OrderedSendMessageExecutor executor) {
        long start = System.currentTimeMillis();
        MessageGroup messageGroup = executor.getMessageGroup();
        try {
            OrderStrategy orderStrategy = OrderStrategyCache.getStrategy(messageGroup.getSubject());
            Connection connection = connectionManager.getConnection(messageGroup);
            sendMessage(connection, produceMessages, executor, orderStrategy);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            timer.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
        }
    }

    private void sendMessage(Connection connection, List<ProduceMessage> messages, SendMessageExecutor executor, OrderStrategy orderStrategy) {
        try {
            ListenableFuture<Map<String, MessageException>> future = connection.sendAsync(messages);
            Futures.addCallback(future, new FutureCallback<Map<String, MessageException>>() {
                @Override
                public void onSuccess(Map<String, MessageException> result) {
                    onSendFinish(messages, result, executor, orderStrategy);
                }

                @Override
                public void onFailure(Throwable t) {
                    Exception ex;
                    if (!(t instanceof Exception)) {
                        ex = new RuntimeException(t);
                    } else {
                        ex = (Exception) t;
                    }
                    onSendError(messages, executor, orderStrategy, ex);
                }
            }, this.executor);
        } catch (Exception e) {
            onSendError(messages, executor, orderStrategy, e);
        }
    }

    private void onSendFinish(List<ProduceMessage> messages, Map<String, MessageException> result, SendMessageExecutor executor, OrderStrategy orderStrategy) {
        try {
            if (result.isEmpty())
                result = Collections.emptyMap();

            for (ProduceMessage pm : messages) {
                MessageException ex = result.get(pm.getMessageId());
                if (ex == null || ex instanceof DuplicateMessageException) {
                    orderStrategy.onSendSuccessful(pm, this, executor, ex);
                } else {
                    //如果是消息被拒绝，说明broker已经限速，不立即重试;
                    if (ex.isBrokerBusy()) {
                        orderStrategy.onSendFailed(pm, this, executor, ex);
                    } else if (ex instanceof BlockMessageException) {
                        //如果是block的,证明还没有被授权,也不重试,task也不重试,需要手工恢复
                        orderStrategy.onSendBlocked(pm, this, executor, ex);
                    } else {
                        Metrics.counter(SEND_MESSAGE_THROWABLE_COUNTER, MetricsConstants.SUBJECT_ARRAY, new String[]{pm.getSubject()}).inc();
                        orderStrategy.onSendError(pm, this, executor, ex);
                    }
                }
            }
        } finally {
            orderStrategy.onSendFinished(messages, this, executor);
        }
    }

    private void onSendError(List<ProduceMessage> messages, SendMessageExecutor executor, OrderStrategy orderStrategy, Exception ex) {
        try {
            for (ProduceMessage pm : messages) {
                Metrics.counter(SEND_MESSAGE_THROWABLE_COUNTER, MetricsConstants.SUBJECT_ARRAY, new String[]{pm.getSubject()}).inc();
                orderStrategy.onSendError(pm, this, executor, ex);
            }
        } finally {
            orderStrategy.onSendFinished(messages, this, executor);
        }
    }
}
