package qunar.tc.qmq.meta.cache;

import qunar.tc.qmq.meta.store.Store;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;

/**
 * The interface Cache manager extend.
 * @description：
 * @author  ：zhixin.zhang
 * @date  ：Created in 下午4:31 2022/2/21
 */
public interface CacheManagerExtend {

	/**
	 * Add request.
	 *
	 * @param request the request
	 */
	void addRequest(MetaInfoRequest request);

	/**
	 * Sets store.
	 *
	 * @param store the store
	 */
	void setStore(Store store);

	/**
	 * Match boolean.
	 *
	 * @param request the request
	 * @return the boolean
	 */
	boolean match(MetaInfoRequest request);

	/**
	 * Name string.
	 *
	 * @return the string
	 */
	String name();
}
