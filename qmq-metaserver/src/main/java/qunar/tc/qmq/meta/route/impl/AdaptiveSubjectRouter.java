package qunar.tc.qmq.meta.route.impl;

import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.meta.BrokerGroup;
import qunar.tc.qmq.meta.route.SubjectRouter;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;

import java.util.List;
import java.util.Map;

/**
 * @author zhenwei.liu
 * @since 2019-08-27
 */
public class AdaptiveSubjectRouter implements SubjectRouter {

    private Map<ClientType, SubjectRouter>

    @Override
    public List<BrokerGroup> route(String realSubject, MetaInfoRequest request) {
        return null;
    }
}
