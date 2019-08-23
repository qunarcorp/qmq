package qunar.tc.qmq.meta.order;

import java.util.Map;

/**
 * @author zhenwei.liu
 * @since 2019-08-23
 */
public interface PartitionMapper {

    Map<Integer, Integer> map(Integer partitionSize, Integer vectorSize);
}
