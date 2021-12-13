package qunar.tc.qmq.meta.route;

import java.util.List;

import qunar.tc.qmq.meta.BrokerGroup;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;

/**
 * The interface Subject route extend.
 * @description：
 * @author  ：zhixin.zhang
 * @date  ：Created in 下午4:06 2021/12/13
 */
public interface SubjectRouteExtend {

	/**
	 * Match list.
	 *
	 * @param realSubject the real subject
	 * @param request the request
	 * @return the list
	 */
	boolean match(final String realSubject, final MetaInfoRequest request);

	/**
	 * Route extend list.
	 *
	 * @param brokerGroups the broker groups
	 * @param realSubject the real subject
	 * @param request the request
	 * @return the list
	 */
	List<BrokerGroup> routeExtend(List<BrokerGroup> brokerGroups,final String realSubject, final MetaInfoRequest request);

}
