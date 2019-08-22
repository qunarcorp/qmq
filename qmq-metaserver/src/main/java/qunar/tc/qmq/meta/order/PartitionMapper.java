package qunar.tc.qmq.meta.order;

import com.google.common.collect.RangeMap;

/**
 * 用于映射 partition
 *
 * @author zhenwei.liu
 * @since 2019-08-22
 */
public interface PartitionMapper {

    /**
     * 将 partitionSize 个 partition 分配到 vectorSize 个 vector 上
     *
     * <p>例如, 要将 30 个 partition 分配到 3 个 brokerGroup 上, 则 partitionSize=30, vectorSize=3
     *
     * @param partitionSize partition 个数
     * @param vectorSize 载体个数
     * @return
     */
    RangeMap<Integer, Integer> map(Integer partitionSize, Integer vectorSize);
}
