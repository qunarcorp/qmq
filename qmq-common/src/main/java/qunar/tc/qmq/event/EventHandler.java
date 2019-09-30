package qunar.tc.qmq.event;

/**
 * @author zhenwei.liu
 * @since 2019-08-28
 */
public interface EventHandler<T> {

    void handle(T event);
}
