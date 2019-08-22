package qunar.tc.qmq.meta.store;

import qunar.tc.qmq.meta.PartitionInfo;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-08-22
 */
public interface PartitionStore {

    void save(PartitionInfo info);

    List<PartitionInfo> getAllLatest();
}
