package qunar.tc.qmq.common;

import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.batch.SendMessageExecutor;
import qunar.tc.qmq.consumer.ConsumeMessageExecutor;
import qunar.tc.qmq.consumer.pull.PulledMessage;
import qunar.tc.qmq.producer.QueueSender;

/**
 * @author zhenwei.liu
 * @since 2019-09-10
 */
public class StrictOrderStrategy extends AbstractOrderStrategy {

    @Override
    void doOnError(ProduceMessage message, QueueSender sender, SendMessageExecutor currentExecutor, Exception e) {

    }

    @Override
    public void onConsumeFailed(PulledMessage message, ConsumeMessageExecutor executor) {
        executor.requeueFirst(message);
    }

    @Override
    public void onMessageNotAcked(PulledMessage message, ConsumeMessageExecutor executor) {
        executor.requeueFirst(message);
    }

    @Override
    public boolean isDeadRetry(int nextRetryCount, BaseMessage message) {
        return true;
    }

    @Override
    public String name() {
        return STRICT;
    }
}
