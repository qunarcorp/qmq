package qunar.tc.qmq;

import qunar.tc.qmq.consumer.MessageExecutor;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.metrics.QmqCounter;
import qunar.tc.qmq.metrics.QmqTimer;

import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_GROUP_ARRAY;

/**
 * @author zhenwei.liu
 * @since 2019-09-03
 */
public abstract class AbstractMessageExecutor implements MessageExecutor {

    private final QmqTimer createToHandleTimer;
    private final QmqTimer handleTimer;
    private final QmqCounter handleFailCounter;

    public AbstractMessageExecutor(String subject, String group) {
        String[] values = {subject, group};
        this.createToHandleTimer = Metrics.timer("qmq_pull_createToHandle_timer", SUBJECT_GROUP_ARRAY, values);
        this.handleTimer = Metrics.timer("qmq_pull_handle_timer", SUBJECT_GROUP_ARRAY, values);
        this.handleFailCounter = Metrics.counter("qmq_pull_handleFail_count", SUBJECT_GROUP_ARRAY, values);
    }

    public QmqTimer getCreateToHandleTimer() {
        return createToHandleTimer;
    }

    public QmqTimer getHandleTimer() {
        return handleTimer;
    }

    public QmqCounter getHandleFailCounter() {
        return handleFailCounter;
    }
}
