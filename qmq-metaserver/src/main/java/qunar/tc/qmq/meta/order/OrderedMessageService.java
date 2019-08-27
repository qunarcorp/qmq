package qunar.tc.qmq.meta.order;

import qunar.tc.qmq.meta.PartitionAllocation;
import qunar.tc.qmq.meta.PartitionMapping;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-08-22
 */
public interface OrderedMessageService {

    void registerOrderedMessage(String subject, int physicalPartitionNum);

    List<PartitionAllocation> getLatestPartitionAllocations();

    List<PartitionMapping> getLatestPartitionMappings();
}
