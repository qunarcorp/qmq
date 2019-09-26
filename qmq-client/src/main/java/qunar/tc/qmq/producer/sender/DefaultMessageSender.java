package qunar.tc.qmq.producer.sender;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import qunar.tc.qmq.MessageGroup;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.common.OrderStrategy;
import qunar.tc.qmq.common.OrderStrategyCache;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.metrics.MetricsConstants;
import qunar.tc.qmq.metrics.QmqTimer;
import qunar.tc.qmq.service.exceptions.BlockMessageException;
import qunar.tc.qmq.service.exceptions.DuplicateMessageException;
import qunar.tc.qmq.service.exceptions.MessageException;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * @author zhenwei.liu
 * @since 2019-09-26
 */
public class DefaultMessageSender implements MessageSender {

    private static final String SEND_MESSAGE_THROWABLE_COUNTER = "qmq_client_producer_send_message_Throwable";

    private ConnectionManager connectionManager;
    private Executor executor;
    private QmqTimer timer;

    public DefaultMessageSender(ConnectionManager connectionManager, Executor executor) {
        this.connectionManager = connectionManager;
        this.executor = executor;
        this.timer = Metrics.timer("qmq_client_producer_send_broker_time");
    }

    @Override
    public void send(List<ProduceMessage> messages, SendMessageExecutor executor, SendMessageExecutorManager sendMessageExecutorManager) {
        long start = System.currentTimeMillis();
        MessageGroup messageGroup = executor.getMessageGroup();
        OrderStrategy orderStrategy = OrderStrategyCache.getStrategy(messageGroup.getSubject());
        try {
            Connection connection = connectionManager.getConnection(messageGroup);
            sendMessage(connection, messages, executor, sendMessageExecutorManager, orderStrategy);
        } catch (Exception e) {
            processSendError(messages, executor, sendMessageExecutorManager, orderStrategy, e);
        } finally {
            timer.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
        }
    }

    private void sendMessage(Connection connection, List<ProduceMessage> messages, SendMessageExecutor executor, SendMessageExecutorManager sendMessageExecutorManager, OrderStrategy orderStrategy) throws Exception {
        ListenableFuture<Map<String, MessageException>> future = connection.sendAsync(messages);
        Futures.addCallback(future, new FutureCallback<Map<String, MessageException>>() {
            @Override
            public void onSuccess(Map<String, MessageException> result) {
                processSendResult(messages, result, executor, sendMessageExecutorManager, orderStrategy);
            }

            @Override
            public void onFailure(Throwable t) {
                Exception ex;
                if (!(t instanceof Exception)) {
                    ex = new RuntimeException(t);
                } else {
                    ex = (Exception) t;
                }
                processSendError(messages, executor, sendMessageExecutorManager, orderStrategy, ex);
            }
        }, this.executor);
    }

    private void processSendResult(List<ProduceMessage> messages, Map<String, MessageException> result, SendMessageExecutor executor, SendMessageExecutorManager sendMessageExecutorManager, OrderStrategy orderStrategy) {
        try {
            if (result.isEmpty())
                result = Collections.emptyMap();

            for (ProduceMessage pm : messages) {
                MessageException ex = result.get(pm.getMessageId());
                if (ex == null || ex instanceof DuplicateMessageException) {
                    orderStrategy.onSendSuccessful(pm, executor, sendMessageExecutorManager);
                } else {
                    //如果是消息被拒绝，说明broker已经限速，不立即重试;
                    if (ex.isBrokerBusy()) {
                        orderStrategy.onSendBrokerError(pm, executor, sendMessageExecutorManager, ex);
                    } else if (ex instanceof BlockMessageException) {
                        //如果是block的,证明还没有被授权,也不重试,task也不重试,需要手工恢复
                        orderStrategy.onSendBlocked(pm, executor, sendMessageExecutorManager, ex);
                    } else {
                        Metrics.counter(SEND_MESSAGE_THROWABLE_COUNTER, MetricsConstants.SUBJECT_ARRAY, new String[]{pm.getSubject()}).inc();
                        orderStrategy.onSendError(pm, executor, sendMessageExecutorManager, ex);
                    }
                }
            }
        } finally {
            orderStrategy.onSendFinished(messages, executor, sendMessageExecutorManager);
        }
    }

    private void processSendError(List<ProduceMessage> messages, SendMessageExecutor executor, SendMessageExecutorManager sendMessageExecutorManager, OrderStrategy orderStrategy, Exception ex) {
        try {
            for (ProduceMessage pm : messages) {
                Metrics.counter(SEND_MESSAGE_THROWABLE_COUNTER, MetricsConstants.SUBJECT_ARRAY, new String[]{pm.getSubject()}).inc();
                orderStrategy.onSendError(pm, executor, sendMessageExecutorManager, ex);
            }
        } finally {
            orderStrategy.onSendFinished(messages, executor, sendMessageExecutorManager);
        }
    }
}
