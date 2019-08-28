package qunar.tc.qmq.meta.order;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author zhenwei.liu
 * @since 2019-08-28
 */
public class PartitionAllocationTask {

    private static final Logger logger = LoggerFactory.getLogger(PartitionAllocationTask.class);

    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("partition-allocation-thread-%s").build()
    );

    private OrderedMessageService orderedMessageService = DefaultOrderedMessageService.getInstance();

    public void start() {
        executor.scheduleWithFixedDelay(() -> {
            try {
                updatePartitionAllocation();
            } catch (Throwable t) {
                logger.error("检查顺序消息分配失败 ", t);
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

    private void updatePartitionAllocation() {

    }
}
