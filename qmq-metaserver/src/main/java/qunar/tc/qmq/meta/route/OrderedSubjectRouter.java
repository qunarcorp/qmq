package qunar.tc.qmq.meta.route;

import qunar.tc.qmq.meta.BrokerGroup;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;

import java.util.Collections;
import java.util.List;

/**
 * 顺序消息不需要这个列表, 需要的是 PartitionAllocation
 *
 * @author zhenwei.liu
 * @since 2019-08-23
 */
public class OrderedSubjectRouter implements SubjectRouter {

    @Override
    public List<BrokerGroup> route(String realSubject, MetaInfoRequest request) {
        return Collections.emptyList();
    }
}
