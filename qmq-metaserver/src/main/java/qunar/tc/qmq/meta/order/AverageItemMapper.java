package qunar.tc.qmq.meta.order;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * <p>将 K 映射到对应的 V 上, 映射策略为平均分, 如下:
 * <pre>
 * 1. 如果 klist.size > vlist.size, 则可能出现同一个 V 多次映射, 如:
 * klist = [1, 2, 3, 4, 5]
 * vlist = [A, B, C]
 * 映射结果
 * result = [1 => A, 2 => B, 3 => C, 4 => A, 5 => B]
 *
 * 2. 如果 klist.size < vlist.size, 则可能出现 V 值不会被完全使用, 如:
 * klist = [1, 2, 3]
 * vlist = [A, B, C, D, E]
 * 映射结果
 * result = [1 => A, 2 => B, 3 =>C]
 * </pre>
 *
 * @author zhenwei.liu
 * @since 2019-08-23
 */
public class AverageItemMapper implements ItemMapper {

    @Override
    public <K, V> Map<K, V> map(List<K> klist, List<V> vlist) {
        Map<K, V> result = Maps.newHashMap();
        for (int kidx = 0; kidx < klist.size(); kidx++) {
            K k = klist.get(kidx);
            int vidx = kidx % vlist.size();
            V v = vlist.get(vidx);
            result.put(k, v);
        }
        return result;
    }
}
