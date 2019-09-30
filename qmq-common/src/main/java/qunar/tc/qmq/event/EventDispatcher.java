package qunar.tc.qmq.event;

import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;

import java.util.Map;

/**
 * @author zhenwei.liu
 * @since 2019-08-28
 */
public class EventDispatcher {

    private static final EventBus eventBus = new EventBus("event-dispatcher");
    private static final Map<Class<? extends EventHandler>, EventHandler> handlerCache = Maps.newConcurrentMap();

    public static boolean register(EventHandler eventHandler) {
        Class<? extends EventHandler> clazz = eventHandler.getClass();
        if (handlerCache.putIfAbsent(clazz, eventHandler) == null) {
            eventBus.register(eventHandler);
            return true;
        }
        return false;
    }

    public static boolean unregister(Class<? extends EventHandler> clazz) {
        return handlerCache.remove(clazz) != null;
    }

    public static void dispatch(Object event) {
        eventBus.post(event);
    }
}
