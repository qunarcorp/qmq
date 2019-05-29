package qunar.tc.qmq.backup.service;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.backup.base.Flushable;
import qunar.tc.qmq.backup.base.ScheduleFlushable;

import java.util.List;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-12-03 10:31
 */
public class ScheduleFlushManager implements ScheduleFlushable {
    private static final Logger LOG = LoggerFactory.getLogger(ScheduleFlushManager.class);

    private List<ScheduleFlushable> flushables = Lists.newArrayList();

    public ScheduleFlushManager() {
    }

    public void register(ScheduleFlushable flushable) {
        flushables.add(flushable);
    }

    @Override
    public void scheduleFlush() {
        flushables.parallelStream().forEach(ScheduleFlushable::scheduleFlush);
    }

    @Override
    public void flush() {
        flushables.parallelStream().forEach(Flushable::flush);
    }

    @Override
    public void destroy() {
        try {
            this.flush();
        } catch (Exception e) {
            LOG.error("schedule flush before close failed.", e);
        }
        flushables.forEach(flushable -> {
            try {
                flushable.destroy();
            } catch (Exception e) {
                LOG.error("close {} failed.", flushable);
            }
        });
    }
}
