package qunar.tc.qmq.base;

import com.google.common.collect.Maps;
import qunar.tc.qmq.broker.OrderStrategy;

import java.util.Map;

/**
 * @author zhenwei.liu
 * @since 2019-08-21
 */
public class OrderStrategyManager {

    private static final Map<String, OrderStrategy> strategyCache = Maps.newConcurrentMap();

    public static OrderStrategy getOrderStrategy(String subject) {
        OrderStrategy orderStrategy = strategyCache.get(subject);
        return orderStrategy == null ? OrderStrategy.BEST_TRIED : orderStrategy;
    }

    public static void setOrderStrategy(String subject, OrderStrategy orderStrategy) {
        strategyCache.put(subject, orderStrategy);
    }
}
