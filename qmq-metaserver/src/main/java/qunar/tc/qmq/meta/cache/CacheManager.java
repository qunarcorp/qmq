package qunar.tc.qmq.meta.cache;

import qunar.tc.qmq.meta.route.ReadonlyBrokerGroupManager;

/**
 * @description： manager先搞一起。
 * @author     ：zhixin.zhang
 * @date       ：Created in 下午9:24 2021/12/30
 */
public class CacheManager {

	private final MetaHeartBeatManager metaHeartBeatManager;

	private  final AliveClientManager aliveClientManager;

	private final CachedMetaInfoManager cachedMetaInfoManager;

	private final CachedOfflineStateManager offlineStateManager;

	private final ReadonlyBrokerGroupManager readonlyBrokerGroupManager;


	public CacheManager(MetaHeartBeatManager metaHeartBeatManager, AliveClientManager aliveClientManager, CachedMetaInfoManager cachedMetaInfoManager, CachedOfflineStateManager offlineStateManager, ReadonlyBrokerGroupManager readonlyBrokerGroupManager) {
		this.metaHeartBeatManager = metaHeartBeatManager;
		this.aliveClientManager = aliveClientManager;
		this.cachedMetaInfoManager = cachedMetaInfoManager;
		this.offlineStateManager = offlineStateManager;
		this.readonlyBrokerGroupManager = readonlyBrokerGroupManager;
	}

	public static CacheManager of(MetaHeartBeatManager metaHeartBeatManager, AliveClientManager aliveClientManager, CachedMetaInfoManager cachedMetaInfoManager,CachedOfflineStateManager offlineStateManager, ReadonlyBrokerGroupManager readonlyBrokerGroupManager) {
		return new CacheManager(metaHeartBeatManager,aliveClientManager,cachedMetaInfoManager,offlineStateManager,readonlyBrokerGroupManager);
	}

	public MetaHeartBeatManager getMetaHeartBeatManager() {
		return metaHeartBeatManager;
	}


	public AliveClientManager getAliveClientManager() {
		return aliveClientManager;
	}


	public CachedMetaInfoManager getCachedMetaInfoManager() {
		return cachedMetaInfoManager;
	}


	public CachedOfflineStateManager getOfflineStateManager() {
		return offlineStateManager;
	}

	public ReadonlyBrokerGroupManager getReadonlyBrokerGroupManager() {
		return readonlyBrokerGroupManager;
	}
}
