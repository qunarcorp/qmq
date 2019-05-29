package qunar.tc.qmq.backup.base;

import qunar.tc.qmq.common.Disposable;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-11-27 20:40
 */
public interface ScheduleFlushable extends Flushable, Disposable {
    void scheduleFlush();
}
