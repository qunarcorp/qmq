package qunar.tc.qmq.meta.order;

import java.util.List;
import java.util.Map;

/**
 * 将每一个 K 映射到多个 V 上
 *
 * @author zhenwei.liu
 * @since 2019-08-22
 */
public interface RangeMapper {

    <K, V> Map<K, List<V>> map(List<K> klist, List<V> vlist);
}
