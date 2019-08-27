package qunar.tc.qmq.config;

import qunar.tc.qmq.meta.PartitionAllocation;

/**
 * @author zhenwei.liu
 * @since 2019-08-19
 */
public interface OrderedMessageManager {

    /**
     * 根据主题获取 partition 数
     *
     * @param subject 主题
     * @return partition 数
     */
    PartitionAllocation getPartitionInfo(String subject);
}
