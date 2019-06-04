package qunar.tc.qmq.backup.service;

import java.util.function.Consumer;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-12-06 17:13
 */
public interface BatchBackup<T> {
    void start();

    void close();

    void add(T t, Consumer<T> consumer);
}
