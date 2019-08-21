package qunar.tc.qmq.batch;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-08-20
 */
public interface OrderedProcessor<Item> {

    void process(List<Item> items, OrderedExecutor<Item> executor);
}
