package qunar.tc.qmq.meta.cache;

import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.PartitionAllocation;
import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.meta.*;
import qunar.tc.qmq.meta.model.SubjectInfo;

import java.util.List;
import java.util.Set;

/**
 * @author zhenwei.liu
 * @since 2019-09-20
 */
public interface CachedMetaInfoManager extends Disposable {

    void refresh();

    SubjectInfo getSubjectInfo(String subject);

    List<String> getConsumerGroups(String subject);

    List<String> getAllBrokerGroupNamesByTag(String tag);

    List<String> getAllDefaultTagBrokerGroupNames();

    BrokerGroup getBrokerGroup(String groupName);

    List<BrokerGroup> getDelayNewGroups();

    PartitionSet getPartitionSet(String subject);

    Partition getPartition(String subject, int partitionId);

    PartitionAllocation getPartitionAllocation(String subject, String consumerGroup);

    Set<String> getBrokerGroupReadonlySubjects(final String brokerGroup);

}
