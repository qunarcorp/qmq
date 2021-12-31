package qunar.tc.qmq.meta.spi.cache;

import java.util.Map;

import com.google.common.collect.Maps;
import qunar.tc.qmq.meta.exception.MetaException;
import qunar.tc.qmq.meta.exception.NoSuchServiceException;
import qunar.tc.qmq.meta.exception.ServiceNotOfRequiredTypeException;

import org.springframework.util.StringUtils;

/**
 * @description：
 * @author     ：zhixin.zhang
 * @date       ：Created in 下午8:35 2021/12/30
 */
public class ClientServiceContext implements QmqServiceContext {


	private Map<String,Object> serviceMap = Maps.newConcurrentMap();


	public void addService(String name, Object service) {
		serviceMap.put(name, service);
	}

	@Override
	public <T> T getServiceByName(String name,Class<T> requiredType) {
		Object service = get(name);
		if (requiredType != null && !requiredType.isInstance(service)) {
			throw new ServiceNotOfRequiredTypeException(name, requiredType, service.getClass());
		}
		return (T)service;
	}

	public Object get(String name) throws MetaException {
		Object obj = serviceMap.get(name);
		if(obj ==null){
			throw new NoSuchServiceException(name,
					"Defined Service are [" + StringUtils.collectionToCommaDelimitedString(this.serviceMap.keySet()) + "]");
		}
		return obj;
	}
}
