package qunar.tc.qmq.backup.service.impl;

import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_ARRAY;

import com.google.common.base.CharMatcher;
import com.google.common.base.Strings;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.store.MessageQueryIndex;
import qunar.tc.qmq.store.event.FixedExecOrderEventBus;

/**
 * @author xiao.liang
 * @since 18 July 2019
 */
public abstract class AbstractEventBusListener implements FixedExecOrderEventBus.Listener<MessageQueryIndex> {

    @Override
    public void onEvent(MessageQueryIndex event) {
        if (event == null) return;
        final String subject = event.getSubject();
        if (isInvisible(subject)) return;
        if (Strings.isNullOrEmpty(event.getMessageId())) return;
        monitorConstructMessage(subject);

        post(event);
    }

    abstract void post(MessageQueryIndex index);

    abstract String getMetricName();

    private static boolean isInvisible(String subject) {
        return Strings.isNullOrEmpty(subject) || CharMatcher.INVISIBLE.matchesAnyOf(subject);
    }

    private void monitorConstructMessage(String subject) {
        Metrics.meter(getMetricName(), SUBJECT_ARRAY, new String[]{subject}).mark();
    }


}
