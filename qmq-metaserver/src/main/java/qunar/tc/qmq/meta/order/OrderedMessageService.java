package qunar.tc.qmq.meta.order;

import qunar.tc.qmq.meta.PartitionAllocation;
import qunar.tc.qmq.meta.PartitionMapping;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-08-22
 */
public interface OrderedMessageService {

    /**
     * 注册新的顺序消息主题
     *
     * @param subject 主题
     * @param physicalPartitionNum 物理分区数量
     */
    void registerOrderedMessage(String subject, int physicalPartitionNum);

    /**
     * 获取正在生效的分区分配列表
     *
     * @return 分区分配列表
     */
    List<PartitionAllocation> getActivatedPartitionAllocations();

    /**
     * 获取最新的分区映射
     *
     * @return 分区映射
     */
    List<PartitionMapping> getLatestPartitionMappings();
}
