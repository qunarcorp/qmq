package qunar.tc.qmq.meta.spi.cache;


import java.util.List;

import qunar.tc.qmq.meta.model.ClientMetaInfo;
import qunar.tc.qmq.meta.model.MetaServerConst;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;

/**
 * The interface Client alive service.
 * @description：
 * @author  ：zhixin.zhang
 * @date  ：Created in 下午8:06 2021/12/30
 */
public interface ClientAliveService extends QmqService {


	/**
	 * Clients by app and sub list.
	 *
	 * @param request the request
	 * @return the list
	 */
	List<ClientMetaInfo> clientsByAppAndSub(MetaInfoRequest request);

	/**
	 * Name string.
	 *
	 * @return the string
	 */
	@Override
	default String name(){
		return MetaServerConst.ServiceName.CLIENT_ALIVE;
	};
}
