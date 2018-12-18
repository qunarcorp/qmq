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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static qunar.tc.qmq.common.StatusSource.*;

/**
 * @author yiqun.fan create on 17-8-18.
 */
public class SwitchWaiter {

    private static final int WAIT_TIMEOUT = 60000;

    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();

    private byte state;

    public SwitchWaiter(boolean initValue) {
        this.state = (byte) (initValue ? 1 : 0);
        this.state |= OPS.getCode();
        this.state |= CODE.getCode();
    }

    public void on(StatusSource src) {
        change(src, true);
    }

    public void off(StatusSource src) {
        change(src, false);
    }

    private void change(StatusSource src, boolean state) {
        lock.lock();
        try {
            boolean ori = (this.state & src.getCode()) == src.getCode();
            if (ori == state) return;

            if (state) {
                this.state |= src.getCode();
            } else {
                this.state &= ((~src.getCode()) & 7);
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

    private boolean isOnline() {
        return this.state == 7;
    }
}
