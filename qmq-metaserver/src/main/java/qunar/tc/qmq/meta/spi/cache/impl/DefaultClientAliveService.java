package qunar.tc.qmq.meta.spi.cache.impl;

import java.util.List;

import qunar.tc.qmq.meta.cache.CacheManager;
import qunar.tc.qmq.meta.model.ClientMetaInfo;
import qunar.tc.qmq.meta.spi.cache.ClientAliveService;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;

/**
 * @description：
 * @author     ：zhixin.zhang
 * @date       ：Created in 下午8:08 2021/12/30
 */
public class DefaultClientAliveService implements ClientAliveService {

	private CacheManager cacheManager;

	public DefaultClientAliveService(CacheManager cacheManager) {
		this.cacheManager = cacheManager;
	}
	/**
	 * Clients by app and sub list.
	 *
	 * @param request the request
	 * @return the list
	 */
	@Override
	public List<ClientMetaInfo> clientsByAppAndSub(MetaInfoRequest request) {
		return null;
	}
}
