package qunar.tc.qmq.sync.master;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.meta.BrokerRegisterService;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author keli.wang
 * @since 2019-03-20
 */
public class SyncLagChecker implements Disposable {
    private static final Logger LOG = LoggerFactory.getLogger(SyncLagChecker.class);

    private final DynamicConfig config;
    private final MasterSyncNettyServer masterSyncNettyServer;
    private final BrokerRegisterService brokerRegisterService;
    private final ScheduledExecutorService executor;

    public SyncLagChecker(final DynamicConfig config, final MasterSyncNettyServer masterSyncNettyServer, final BrokerRegisterService brokerRegisterService) {
        this.config = config;
        this.masterSyncNettyServer = masterSyncNettyServer;
        this.brokerRegisterService = brokerRegisterService;
        this.executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("sync-lag-checker"));
    }

    public void start() {
        executor.scheduleAtFixedRate(this::checkLag, 1, 1, TimeUnit.SECONDS);
    }

    private void checkLag() {
        try {
            final int maxLagSeconds = config.getInt("slave.message.lag.time.max.seconds", 5);
            final long caughtUpTime = masterSyncNettyServer.getMessageLastCaughtUpTime();
            final long now = System.currentTimeMillis();

            if (now - caughtUpTime <= TimeUnit.SECONDS.toMillis(maxLagSeconds)) {
                brokerRegisterService.unmarkReadonly();
                return;
            }

            if (!config.getBoolean("wait.slave.wrote", false)) {
                brokerRegisterService.unmarkReadonly();
                return;
            }

            if (config.getBoolean("slave.message.lag.mark_readonly", false)) {
                brokerRegisterService.markReadonly();
            } else {
                brokerRegisterService.unmarkReadonly();
            }
        } catch (Throwable e) {
            LOG.error("sync lag check failed.", e);
        }
    }

    @Override
    public void destroy() {
        executor.shutdown();
    }
}
