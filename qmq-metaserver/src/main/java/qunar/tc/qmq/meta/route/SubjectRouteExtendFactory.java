package qunar.tc.qmq.meta.route;

import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;

import com.google.common.collect.Lists;
import qunar.tc.qmq.meta.BrokerGroup;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;

/**
 * @description：
 * @author     ：zhixin.zhang
 * @date       ：Created in 下午4:07 2021/12/13
 */
public class SubjectRouteExtendFactory {

	private static List<SubjectRouteExtend> subjectRouteExtends = Lists.newArrayList();

	static {
		ServiceLoader<SubjectRouteExtend> services = ServiceLoader.load(SubjectRouteExtend.class);
		for (SubjectRouteExtend instance : services) {
			subjectRouteExtends.add(instance);
		}
	}

	public static List<BrokerGroup> routeExtend(List<BrokerGroup> brokerGroups,final MetaInfoRequest request){
		if (brokerGroups == null || brokerGroups.isEmpty()) {
			return Collections.emptyList();
		}
		if (subjectRouteExtends.size() == 0) {
			return brokerGroups;
		}
		List<BrokerGroup> broker = brokerGroups;
		for (SubjectRouteExtend subjectRouteExtend : subjectRouteExtends) {
			if(subjectRouteExtend.match(request)){
				broker = subjectRouteExtend.routeExtend(broker,request);
			}
		}
		return broker;
	};
}
