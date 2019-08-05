package qunar.tc.qmq.store;

import java.util.List;
import java.util.Objects;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.protocol.consumer.PullFilter;
import qunar.tc.qmq.protocol.consumer.PullFilterType;
import qunar.tc.qmq.protocol.consumer.PullRequest;
import qunar.tc.qmq.store.buffer.Buffer;

/**
 * @author zhenwei.liu
 * @since 2019-08-01
 */
public interface PullMessageFilter {

	boolean isActive(PullRequest request, Buffer message);

	boolean match(PullRequest request, Buffer message);

	void init(DynamicConfig config);

	@SuppressWarnings("unchecked")
	default <T> T getFilter(PullRequest request, PullFilterType type) {
		List<PullFilter> filterSwitches = request.getFilters();
		if (filterSwitches == null) {
			return null;
		}
		for (PullFilter filterSwitch : filterSwitches) {
			if (Objects.equals(type, filterSwitch.type())) {
				return (T) filterSwitch;
			}
		}
		return null;
	}
}
