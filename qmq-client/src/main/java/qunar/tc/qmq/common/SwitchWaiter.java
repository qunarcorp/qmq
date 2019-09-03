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

package qunar.tc.qmq.common;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.StatusSource;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static qunar.tc.qmq.StatusSource.CODE;
import static qunar.tc.qmq.StatusSource.OPS;

/**
 * @author yiqun.fan create on 17-8-18.
 */
public class SwitchWaiter {

    public interface Listener {
        void onStateChange(boolean isOnline);
    }

    private static final int WAIT_TIMEOUT = 60000;
    private static final Logger logger = LoggerFactory.getLogger(SwitchWaiter.class);

    private List<Listener> listeners = Lists.newArrayList();

    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();

    private byte onlineResult;

    public SwitchWaiter(boolean healthcheckOnlineState) {
        this.onlineResult = (byte) (healthcheckOnlineState ? 1 : 0);
        this.onlineResult |= OPS.getCode();
        this.onlineResult |= CODE.getCode();
    }

    public void on(StatusSource src) {
        change(src, true);
    }

    public void off(StatusSource src) {
        change(src, false);
    }

    private void change(StatusSource src, boolean sourceOnline) {
        lock.lock();
        boolean oldOnlineState = isOnline();
        try {
            boolean isSameSourceAndOnline = (this.onlineResult & src.getCode()) == src.getCode();
            if (isSameSourceAndOnline == sourceOnline) {
                // 本来就是上线, 不需要变更
                return;
            }

            if (sourceOnline) {
                this.onlineResult |= src.getCode();
            } else {
                this.onlineResult &= ((~src.getCode()) & 7);
            }

            boolean currentOnlineState = isOnline();
            if (oldOnlineState != currentOnlineState) {
                for (Listener listener : listeners) {
                    try {
                        listener.onStateChange(currentOnlineState);
                    } catch (Throwable t) {
                        logger.error("上下线回调失败 old state {} new state {}", oldOnlineState, currentOnlineState, t);
                    }
                }
            }

            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public boolean waitOn() {
        lock.lock();
        try {
            while (!isOnline()) {
                try {
                    condition.await(WAIT_TIMEOUT, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        } finally {
            lock.unlock();
        }
        return true;
    }

    public boolean isOnline() {
        return this.onlineResult == 7;
    }

    public void addListener(Listener listener) {
        this.listeners.add(listener);
    }
}
