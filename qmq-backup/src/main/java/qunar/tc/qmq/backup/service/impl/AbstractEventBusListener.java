package qunar.tc.qmq.backup.service.impl;

import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_ARRAY;

import com.google.common.base.CharMatcher;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.store.event.FixedExecOrderEventBus;

/**
 * TODO completion javadoc.
 *
 * @author xiao.liang
 * @since 18 July 2019
 */
public abstract class AbstractEventBusListener<T> implements FixedExecOrderEventBus.Listener<T> {

	@Override
	public void onEvent(T event) {
		if (event == null) return;
		final String subject = getSubject(event);
		if (isInvisible(subject)) return;
		monitorConstructMessage(subject);

		post(event);
	}

	abstract String getSubject(T event);

	abstract void post(T index) ;

	abstract String getMetricName();

	private static boolean isInvisible(String subject) {
		return CharMatcher.INVISIBLE.matchesAnyOf(subject);
	}

	private  void monitorConstructMessage(String subject) {
		Metrics.meter(getMetricName(), SUBJECT_ARRAY, new String[] {subject}).mark();
	}



}
