package qunar.tc.qmq.producer.sender;

import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.batch.SendMessageExecutor;
import qunar.tc.qmq.consumer.ConsumeMessageExecutor;
import qunar.tc.qmq.consumer.pull.PulledMessage;
import qunar.tc.qmq.producer.QueueSender;

/**
 * @author zhenwei.liu
 * @since 2019-09-10
 */
public class StrictOrderStrategy extends AbstractOrderStrategy {

    private static final String NAME = "STRICT";
    private static final String LOCAL_RETRY = "__local_retry";
    private static final int MAX_RETRY = 3;

    @Override
    void doOnError(ProduceMessage message, QueueSender sender, SendMessageExecutor currentExecutor, Exception e) {

    }

    @Override
    public void onConsumeFailed(PulledMessage message, ConsumeMessageExecutor executor) {
        int currentRetry = message.getIntProperty(LOCAL_RETRY);
        if (currentRetry < MAX_RETRY) {
            executor.requeueFirst(message);
            message.setProperty(LOCAL_RETRY, ++currentRetry);
        }
    }

    @Override
    public String name() {
        return NAME;
    }
}
