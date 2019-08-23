package qunar.tc.qmq.meta.order;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * @author zhenwei.liu
 * @since 2019-08-23
 */
public class AveragePartitionMapper implements PartitionMapper {

    @Override
    public Map<Integer, Integer> map(Integer partitionSize, Integer vectorSize) {
        Map<Integer, Integer> result = Maps.newHashMap();
        for (int i = 0; i < partitionSize; i++) {
            result.put(i, i % vectorSize);
        }
        return result;
    }
}
