package qunar.tc.qmq.meta.spi.cache.impl;

import java.util.List;
import java.util.stream.Collectors;

import qunar.tc.qmq.meta.cache.CacheManager;
import qunar.tc.qmq.meta.cache.MetaHeartBeatManager;
import qunar.tc.qmq.meta.model.ClientMetaInfo;
import qunar.tc.qmq.meta.spi.cache.ClientAliveService;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;

import org.springframework.util.CollectionUtils;

/**
 * @description：
 * @author     ：zhixin.zhang
 * @date       ：Created in 下午8:08 2021/12/30
 */
public class DefaultClientAliveServiceImpl implements ClientAliveService {

	private final CacheManager cacheManager;


	public DefaultClientAliveServiceImpl(CacheManager cacheManager) {
		this.cacheManager = cacheManager;
	}
	/**
	 * Clients by app and sub list.
	 *
	 * @param request the request
	 * @return the list
	 */
	@Override
	public List<ClientMetaInfo> aliveClientsByAppAndSub(MetaInfoRequest request,long timeOut) {
		List<ClientMetaInfo> list = metaHeartManager().clientsByAppAndSub(request);
		return getTimeout(list, timeOut);
	}

	@Override
	public List<ClientMetaInfo> aliveClientsBySub(MetaInfoRequest request, long timeOut) {
		List<ClientMetaInfo> list = metaHeartManager().clientsBySubject(request);
		return getTimeout(list, timeOut);
	}

	private List<ClientMetaInfo> getTimeout(List<ClientMetaInfo> list,long timeOut){
		if (!CollectionUtils.isEmpty(list)) {
			return list.stream().filter(s -> System.currentTimeMillis() - s.getUpdateTime().getTime() < timeOut)
					.collect(Collectors.toList());
		}
		return null;
	}

	private MetaHeartBeatManager metaHeartManager(){
		return cacheManager.getMetaHeartBeatManager();
	}
}
