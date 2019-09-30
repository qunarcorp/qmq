package qunar.tc.qmq.common;

import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.producer.sender.SendMessageExecutor;
import qunar.tc.qmq.consumer.ConsumeMessageExecutor;
import qunar.tc.qmq.consumer.pull.PulledMessage;
import qunar.tc.qmq.producer.sender.SendMessageExecutorManager;
import qunar.tc.qmq.service.exceptions.MessageException;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-08-21
 */
public interface OrderStrategy {

    String BEST_TRIED = "BEST_TRIED";
    String STRICT = "STRICT";

    /**
     * 发送错误, 如网络异常等第三方因素
     */
    void onSendError(ProduceMessage message, SendMessageExecutor currentExecutor, SendMessageExecutorManager sendMessageExecutorManager, Exception e);

    /**
     * 发送错误, 代表 Broker 出现的问题
     */
    void onSendBrokerError(ProduceMessage pm, SendMessageExecutor currentExecutor, SendMessageExecutorManager sendMessageExecutorManager, Exception e);

    void onSendBlocked(ProduceMessage pm, SendMessageExecutor currentExecutor, SendMessageExecutorManager sendMessageExecutorManager, MessageException ex);

    void onSendSuccess(ProduceMessage pm, SendMessageExecutor currentExecutor, SendMessageExecutorManager sendMessageExecutorManager);

    void onSendFinished(List<ProduceMessage> sourceMessages, SendMessageExecutor currentExecutor, SendMessageExecutorManager sendMessageExecutorManager);

    void onConsumeSuccess(PulledMessage message, ConsumeMessageExecutor executor);

    void onConsumeFailed(PulledMessage message, ConsumeMessageExecutor executor, Throwable t);

    void onMessageNotAcked(PulledMessage message, ConsumeMessageExecutor executor);

    boolean isDeadRetry(int nextRetryCount, BaseMessage message);

    String name();
}
