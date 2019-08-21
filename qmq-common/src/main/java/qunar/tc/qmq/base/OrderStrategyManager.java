package qunar.tc.qmq.base;

import qunar.tc.qmq.OrderStrategy;

/**
 * @author zhenwei.liu
 * @since 2019-08-21
 */
public interface OrderStrategyManager {

    OrderStrategy getOrderStrategy(String subject);
}
