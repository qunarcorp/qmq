package qunar.tc.qmq.meta.store;

import qunar.tc.qmq.meta.order.PartitionSet;

/**
 * @author zhenwei.liu
 * @since 2019-08-27
 */
public interface PartitionSetStore {

    int save(PartitionSet partitionSet);

    PartitionSet selectByVersion(String subject, String version);
}
