package qunar.tc.qmq.meta.order;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;

/**
 * <pre>
 * 平均映射器, 如 partitionSize=30, vectorSize=3, 则最终映射结果是:
 * [0, 10) => 0
 * [10, 20) => 1
 * [20, 30) => 2
 * </pre>
 *
 * @author zhenwei.liu
 * @since 2019-08-22
 */
public class AverageRangePartitionMapper implements RangePartitionMapper {

    @Override
    public RangeMap<Integer, Integer> map(Integer partitionSize, Integer vectorSize) {
        RangeMap<Integer, Integer> map = TreeRangeMap.create();
        // 每个 vector 需要承载的 partition 数
        int partitionRange = (int) Math.ceil((double) partitionSize / vectorSize);

        int startIndex = 0;
        for (int i = 0; i < vectorSize; i++) {
            int endIndex = startIndex + partitionRange;
            if (endIndex > partitionSize) {
                endIndex = partitionSize;
            }
            map.put(Range.closedOpen(startIndex, endIndex), i);
            startIndex += partitionRange;
        }

        return map;
    }
}
