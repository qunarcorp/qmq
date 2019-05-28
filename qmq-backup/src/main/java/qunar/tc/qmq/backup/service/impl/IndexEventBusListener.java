package qunar.tc.qmq.backup.service.impl;

import com.google.common.base.CharMatcher;
import qunar.tc.qmq.backup.service.BatchBackup;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.store.MessageQueryIndex;
import qunar.tc.qmq.store.event.FixedExecOrderEventBus;
import qunar.tc.qmq.utils.RetrySubjectUtils;

import java.util.function.Consumer;

import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_ARRAY;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019/5/28
 */
public class IndexEventBusListener implements FixedExecOrderEventBus.Listener<MessageQueryIndex> {
    private final BatchBackup<MessageQueryIndex> deadMessageBatchBackup;
    private final BatchBackup<MessageQueryIndex> deadRecordBatchBackup;
    private final BatchBackup<MessageQueryIndex> indexBatchBackup;
    private final Consumer<MessageQueryIndex> consumer;

    public IndexEventBusListener(BatchBackup<MessageQueryIndex> deadMessageBatchBackup,
                                 BatchBackup<MessageQueryIndex> deadRecordBatchBackup,
                                 BatchBackup<MessageQueryIndex> indexBatchBackup,
                                 Consumer<MessageQueryIndex> consumer) {
        this.deadMessageBatchBackup = deadMessageBatchBackup;
        this.deadRecordBatchBackup = deadRecordBatchBackup;
        this.indexBatchBackup = indexBatchBackup;
        this.consumer = consumer;
    }

    @Override
    public void onEvent(MessageQueryIndex event) {
        if (event == null) return;
        final String subject = event.getSubject();
        if (isInvisible(subject)) return;
        monitorConstructMessage(subject);

        post(event);
    }

    private void post(MessageQueryIndex index) {
        // handle message attributes
        if (RetrySubjectUtils.isDeadRetrySubject(index.getSubject())) {
            // additionally, generate a consume record
            saveDeadMessage(index, consumer);
            return;
        }
        // indexBatchBackup
        indexBatchBackup.add(index, consumer);
    }

    private void saveDeadMessage(MessageQueryIndex message, Consumer<MessageQueryIndex> consumer) {
        deadMessageBatchBackup.add(message, consumer);
        deadRecordBatchBackup.add(message, null);
    }

    private static boolean isInvisible(String subject) {
        return CharMatcher.INVISIBLE.matchesAnyOf(subject);
    }

    private static void monitorConstructMessage(String subject) {
        Metrics.meter("construct.message.qps",SUBJECT_ARRAY,new String[]{subject}).mark();
    }
}
