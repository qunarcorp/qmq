/*
 * Copyright 2018 Qunar, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package qunar.tc.qmq.delay.cleaner;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.delay.Switchable;
import qunar.tc.qmq.delay.config.StoreConfiguration;
import qunar.tc.qmq.delay.store.log.DispatchLog;
import qunar.tc.qmq.delay.store.log.MessageLog;
import qunar.tc.qmq.delay.store.log.ScheduleLog;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-07-30 15:56
 */
public class LogCleaner implements Switchable {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogCleaner.class);

    private final DispatchLog dispatchLog;
    private final MessageLog messageLog;
    private final ScheduleLog scheduleLog;
    private final StoreConfiguration config;
    private final ScheduledExecutorService cleanScheduler;

    public LogCleaner(StoreConfiguration config, DispatchLog dispatchLog, ScheduleLog scheduleLog, MessageLog messageLog) {
        this.config = config;
        this.scheduleLog = scheduleLog;
        this.dispatchLog = dispatchLog;
        this.messageLog = messageLog;

        this.cleanScheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("delay-broker-cleaner-%d").build());
    }

    private void cleanMessageLog() {
        messageLog.clean();
    }

    private void cleanDispatchLog(CleanHook hook) {
        dispatchLog.clean(hook);
    }

    private void cleanScheduleOldLog() {
        scheduleLog.clean();
    }

    private void clean() {
        if (!config.isDeleteExpiredLogsEnable()) return;
        try {
            cleanMessageLog();
            cleanDispatchLog(scheduleLog::clean);
            cleanScheduleOldLog();
        } catch (Throwable e) {
            LOGGER.error("LogCleaner exec clean error.", e);
        }
    }

    @Override
    public void start() {
        cleanScheduler.scheduleAtFixedRate(this::clean, 0, config.getLogCleanerIntervalSeconds(), TimeUnit.SECONDS);
    }

    @Override
    public void shutdown() {
        cleanScheduler.shutdown();
        try {
            cleanScheduler.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("Shutdown log cleaner scheduler interrupted.");
        }
    }

    public interface CleanHook {
        boolean clean(long key);
    }

}
