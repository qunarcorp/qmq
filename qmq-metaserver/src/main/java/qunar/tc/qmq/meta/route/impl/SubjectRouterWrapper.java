package qunar.tc.qmq.meta.route.impl;

import java.util.List;

import qunar.tc.qmq.meta.BrokerGroup;
import qunar.tc.qmq.meta.route.SubjectRouteExtendFactory;
import qunar.tc.qmq.meta.route.SubjectRouter;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;

/**
 * @description：
 * @author     ：zhixin.zhang
 * @date       ：Created in 下午3:59 2021/12/15
 */
public class SubjectRouterWrapper implements SubjectRouter {

	private SubjectRouter subjectRouter;


	public SubjectRouterWrapper(SubjectRouter subjectRouter) {
		this.subjectRouter = subjectRouter;
	}

	@Override
	public List<BrokerGroup> route(String realSubject, MetaInfoRequest request) {
		return routeExtend(subjectRouter.route(realSubject, request), request);
	}
	/**
	 * 根据请求分配
	 * @param brokerGroups
	 * @param request
	 * @return
	 */
	private List<BrokerGroup> routeExtend(List<BrokerGroup> brokerGroups, final MetaInfoRequest request) {
		return SubjectRouteExtendFactory.routeExtend(brokerGroups, request);
	}
}
