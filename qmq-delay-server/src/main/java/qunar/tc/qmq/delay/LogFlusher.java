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

package qunar.tc.qmq.delay;

import qunar.tc.qmq.delay.store.IterateOffsetManager;
import qunar.tc.qmq.delay.store.log.DispatchLog;
import qunar.tc.qmq.delay.store.log.MessageLog;
import qunar.tc.qmq.store.PeriodicFlushService;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-07-19 17:14
 */
public class LogFlusher implements Switchable {
    private final PeriodicFlushService messageLogFlushService;
    private final PeriodicFlushService dispatchLogFlushService;
    private final PeriodicFlushService iterateOffsetFlushService;

    LogFlusher(MessageLog messageLog, IterateOffsetManager offsetManager, DispatchLog dispatchLog) {
        this.messageLogFlushService = new PeriodicFlushService(messageLog.getProvider());
        this.dispatchLogFlushService = new PeriodicFlushService(dispatchLog.getProvider());
        this.iterateOffsetFlushService = new PeriodicFlushService(offsetManager.getFlushProvider());
    }

    @Override
    public void start() {
        messageLogFlushService.start();
        dispatchLogFlushService.start();
        iterateOffsetFlushService.start();
    }

    @Override
    public void shutdown() {
        messageLogFlushService.close();
        dispatchLogFlushService.close();
        iterateOffsetFlushService.close();
    }

}
