package qunar.tc.qmq.meta.loadbalance;

import java.util.Map;
import java.util.ServiceLoader;

import com.google.common.collect.Maps;

/**
 * @description：定义loadbalance
 * @author     ：zhixin.zhang
 * @date       ：Created in 下午4:07 2021/12/13
 */
public class LoadBalanceFactory {

	private static Map<String,LoadBalance> loadBalances = Maps.newHashMap();

	private static RandomLoadBalance defaultLoadBalance = new RandomLoadBalance();

	static {
		ServiceLoader<LoadBalance> services = ServiceLoader.load(LoadBalance.class);
		for (LoadBalance instance : services) {
			loadBalances.put(instance.name(), instance);
		}
	}

	public static LoadBalance getByName(String name) {
		return loadBalances.containsKey(name) ? loadBalances.get(name) : defaultLoadBalance;
	}

}
