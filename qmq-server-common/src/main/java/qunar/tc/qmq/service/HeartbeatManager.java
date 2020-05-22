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

package qunar.tc.qmq.service;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.concurrent.NamedThreadFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * @author yunfeng.yang
 * @since 2017/9/19
 */
public class HeartbeatManager<T> implements Disposable {
    private final HashedWheelTimer timer;

    private final ConcurrentMap<T, Timeout> timeouts;

    public HeartbeatManager() {
        this.timeouts = new ConcurrentHashMap<>();
        this.timer = new HashedWheelTimer(new NamedThreadFactory("qmq-heartbeat"));
        this.timer.start();
    }

    public void cancel(T key) {
        Timeout timeout = timeouts.remove(key);
        if (timeout == null) return;

        timeout.cancel();
    }

    public void refreshHeartbeat(T key, TimerTask task, long timeout, TimeUnit unit) {
        Timeout context = timer.newTimeout(task, timeout, unit);
        final Timeout old = timeouts.put(key, context);
        if (old != null && !old.isCancelled() && !old.isExpired()) {
            old.cancel();
        }
    }

    @Override
    public void destroy() {
        timer.stop();
    }
}
