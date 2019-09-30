package qunar.tc.qmq.common;

import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;

import java.util.Map;

/**
 * @author zhenwei.liu
 * @since 2019-09-08
 */
public class VersionableComponentManager {

    private static Map<Class, RangeMap<Integer, Object>> componentMap = Maps.newConcurrentMap();

    public static <T> T getComponent(Class<T> clazz, int version) {
        RangeMap<Integer, Object> rangeMap = componentMap.get(clazz);
        if (rangeMap == null) {
            throw new IllegalStateException(String.format("无法找到 component class %s", clazz));
        }
        Object component = rangeMap.get(version);
        if (component == null) {
            throw new IllegalStateException(String.format("无法找到 component class %s version %s", clazz, version));
        }
        return (T) component;
    }

    public static <T> void registerComponent(Class<T> clazz, Range<Integer> versionRange, T instance) {
        RangeMap<Integer, Object> rangeMap = componentMap.computeIfAbsent(clazz, key -> TreeRangeMap.create());
        rangeMap.put(versionRange, instance);
    }
}
