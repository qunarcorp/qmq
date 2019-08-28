package qunar.tc.qmq.meta.order;

import java.util.List;
import java.util.Map;

/**
 * 将每一个 K 的值映射到一个 V 值上, 必须将所有的 K 映射完
 *
 * @author zhenwei.liu
 * @since 2019-08-23
 */
public interface ItemMapper {

    <K, V> Map<K, V> map(List<K> klist, List<V> vlist);
}
