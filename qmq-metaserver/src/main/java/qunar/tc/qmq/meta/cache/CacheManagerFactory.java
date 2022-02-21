package qunar.tc.qmq.meta.cache;

import java.util.List;
import java.util.ServiceLoader;

import com.google.common.collect.Lists;
import qunar.tc.qmq.meta.store.Store;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;

import org.springframework.util.CollectionUtils;

/**
 * @description：
 * @author     ：zhixin.zhang
 * @date       ：Created in 下午4:33 2022/2/21
 */
public class CacheManagerFactory {
	private static List<CacheManagerExtend> cacheManagerExtends = Lists.newArrayList();

	static {
		ServiceLoader<CacheManagerExtend> services = ServiceLoader.load(CacheManagerExtend.class);
		for (CacheManagerExtend instance : services) {
			cacheManagerExtends.add(instance);
		}
	}

	public static void addRequest(MetaInfoRequest request){
		if (request ==null || CollectionUtils.isEmpty(cacheManagerExtends)) {
			return;
		}
		for (CacheManagerExtend cacheManagerExtend : cacheManagerExtends) {
			if(cacheManagerExtend.match(request)){
				cacheManagerExtend.addRequest(request);
			}
		}
	};

	public static List<CacheManagerExtend> getCacheManagerExtends() {
		return cacheManagerExtends;
	}

	public static CacheManagerExtend getManager(String name) {
		if (CollectionUtils.isEmpty(cacheManagerExtends)) {
			return null;
		}
		for (CacheManagerExtend cacheManagerExtend : cacheManagerExtends) {
			if(name.equalsIgnoreCase(cacheManagerExtend.name())){
				return cacheManagerExtend;
			}
		}
		return null;
	}

	public static void setStore(Store store) {
		if (CollectionUtils.isEmpty(cacheManagerExtends)) {
			return;
		}
		for (CacheManagerExtend cacheManagerExtend : cacheManagerExtends) {
			cacheManagerExtend.setStore(store);
		}
	}

}
