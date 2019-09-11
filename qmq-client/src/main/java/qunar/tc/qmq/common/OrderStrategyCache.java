package qunar.tc.qmq.common;

import com.google.common.collect.Maps;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.producer.sender.DefaultMessageGroupResolver;
import qunar.tc.qmq.producer.sender.MessageGroupResolver;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author zhenwei.liu
 * @since 2019-09-11
 */
public class OrderStrategyCache {

    private static final AtomicBoolean inited = new AtomicBoolean(false);
    private static final Map<String, OrderStrategy> orderStrategyCache = Maps.newConcurrentMap();
    private static OrderStrategy defaultStrategy;

    public static void initOrderStrategy(MessageGroupResolver messageGroupResolver) {
        if (inited.compareAndSet(false, true)) {
            BestTriedOrderStrategy bestTriedOrderStrategy = new BestTriedOrderStrategy(messageGroupResolver);
            StrictOrderStrategy strictOrderStrategy = new StrictOrderStrategy();
            setDefaultStrategy(bestTriedOrderStrategy);
            registerStrategy(bestTriedOrderStrategy);
            registerStrategy(strictOrderStrategy);
        }
    }

    private static void setDefaultStrategy(OrderStrategy orderStrategy) {
        defaultStrategy = orderStrategy;
    }

    private static void registerStrategy(OrderStrategy strategy) {
        orderStrategyCache.put(strategy.name(), strategy);
    }

    public static OrderStrategy getStrategy(String subject) {
        String strategyName = OrderStrategyManager.getOrderStrategy(subject);
        if (strategyName == null) {
            return defaultStrategy;
        }
        OrderStrategy orderStrategy = orderStrategyCache.get(strategyName);
        if (orderStrategy == null) {
            return defaultStrategy;
        }
        return orderStrategy;
    }
}
