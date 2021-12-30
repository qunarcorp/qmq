package qunar.tc.qmq.meta.spi.cache;

/**
 * The interface Qmq service context.
 *
 * @description：
 * @author  ：zhixin.zhang
 * @date  ：Created in 下午8:24 2021/12/30
 */
public interface QmqServiceContext {


	/**
	 * Gets service by name.
	 *
	 * @param <T>   the type parameter
	 * @param name the name
	 * @param requiredType the required type
	 * @return the service by name
	 */
	<T> T getServiceByName(String name,Class<T> requiredType);

}
