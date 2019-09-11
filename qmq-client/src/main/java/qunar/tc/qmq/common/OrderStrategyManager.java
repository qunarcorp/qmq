package qunar.tc.qmq.common;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * @author zhenwei.liu
 * @since 2019-08-21
 */
public class OrderStrategyManager {

    private static final Map<String, String> strategyCache = Maps.newConcurrentMap();

    public static String getOrderStrategy(String subject) {
        return strategyCache.get(subject);
    }

    public static void setOrderStrategy(String subject, String strategyName) {
        strategyCache.put(subject, strategyName);
    }
}
