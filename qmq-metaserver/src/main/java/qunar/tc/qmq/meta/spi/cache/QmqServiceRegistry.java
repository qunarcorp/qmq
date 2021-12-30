package qunar.tc.qmq.meta.spi.cache;

/**
 * The interface Client cache.
 * @description：
 * @author  ：zhixin.zhang
 * @date  ：Created in 下午8:01 2021/12/30
 */
public interface QmqServiceRegistry {
	/**
	 * Registry.
	 *
	 * @param clientService the client service
	 */
	void registry(QmqServiceContext clientService);
}
