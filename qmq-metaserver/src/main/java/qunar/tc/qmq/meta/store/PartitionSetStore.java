package qunar.tc.qmq.meta.store;

import qunar.tc.qmq.meta.PartitionSet;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-08-27
 */
public interface PartitionSetStore {

    int save(PartitionSet partitionSet);

    PartitionSet getByVersion(String subject, String version);

    PartitionSet getLatest(String subject);

    List<PartitionSet> getLatest();
}
