package qunar.tc.qmq.meta.spi.cache;

import java.util.List;
import java.util.ServiceLoader;

import com.google.common.collect.Lists;

/**
 * @description：
 * @author     ：zhixin.zhang
 * @date       ：Created in 下午8:15 2021/12/30
 */
public class ClientCacheManager {

	public static List<QmqServiceRegistry> clientServiceRegistries = Lists.newArrayList();

	static {
		ServiceLoader<QmqServiceRegistry> services = ServiceLoader.load(QmqServiceRegistry.class);
		for (QmqServiceRegistry registry : services) {
			clientServiceRegistries.add(registry);
		}
	}

	public static void registry(QmqServiceContext qmqServiceContext) {
		for (QmqServiceRegistry clientServiceRegistry : clientServiceRegistries) {
			clientServiceRegistry.registry(qmqServiceContext);
		}
	}

}
