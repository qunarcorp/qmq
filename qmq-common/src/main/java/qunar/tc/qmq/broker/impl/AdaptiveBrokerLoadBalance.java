package qunar.tc.qmq.broker.impl;

import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.base.BaseMessage.keys;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.BrokerLoadBalance;

/**
 * @author zhenwei.liu
 * @since 2019-08-20
 */
public class AdaptiveBrokerLoadBalance implements BrokerLoadBalance {

	private static final String DEFAULT_LOAD_BALANCE = PollBrokerLoadBalance.class.getName();

	private Map<String, BrokerLoadBalance> loadBalanceMap = Maps.newHashMap();

	public AdaptiveBrokerLoadBalance() {
		for (BrokerLoadBalance loadBalance : ServiceLoader.load(BrokerLoadBalance.class)) {
			loadBalanceMap.put(loadBalance.getClass().getName(), loadBalance);
		}
	}

	@Override
	public BrokerGroupInfo loadBalance(BrokerClusterInfo cluster, BrokerGroupInfo lastGroup,
			List<BaseMessage> messages) {
		BaseMessage message = messages.get(0);
		return selectLoadBalance(message).loadBalance(cluster, lastGroup, messages);
	}

	private BrokerLoadBalance selectLoadBalance(BaseMessage message) {
		Object loadBalanceType = message.getProperty(keys.qmq_queueLoadBalanceType);
		if (loadBalanceType == null) {
			return loadBalanceMap.get(DEFAULT_LOAD_BALANCE);
		}
		return loadBalanceMap.get(loadBalanceType.toString());
	}
}
