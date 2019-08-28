package qunar.tc.qmq.meta.order;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * 将每个 K 平均映射到多个 V 上, 如
 * <pre>
 * klist = [1, 2, 3]
 * vlist = [A, B, C, D, E, F, G]
 * 映射结果
 * result = [1 => {A, B, C}, 2 => {D, E, F}, 3 => {G}]
 * </pre>
 *
 * @author zhenwei.liu
 * @since 2019-08-22
 */
public class AverageRangeMapper implements RangeMapper {

    @Override
    public <K, V> Map<K, List<V>> map(List<K> klist, List<V> vlist) {
        Map<K, List<V>> result = Maps.newHashMap();
        int ksize = klist.size();
        int vsize = vlist.size();
        int range = (int) Math.round((double) vsize / ksize);
        if (range == 0) {
            range = 1;
        }

        int vidx = 0;
        for (int i = 0; i < ksize; i++) {
            List<V> vsubList = vlist.subList(vidx, vidx + range);
            result.put(klist.get(i), vsubList);
            vidx += range;
        }

        return result;
    }
}
