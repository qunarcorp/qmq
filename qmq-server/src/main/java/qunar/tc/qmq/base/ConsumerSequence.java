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

package qunar.tc.qmq.base;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author yunfeng.yang
 * @since 2017/8/1
 */
public class ConsumerSequence {
    private final AtomicLong pullSequence;
    private final AtomicLong ackSequence;

    private final Lock pullLock = new ReentrantLock();
    private final Lock ackLock = new ReentrantLock();

    public ConsumerSequence(final long pullSequence, final long ackSequence) {
        this.pullSequence = new AtomicLong(pullSequence);
        this.ackSequence = new AtomicLong(ackSequence);
    }

    public long getPullSequence() {
        return pullSequence.get();
    }

    public void setPullSequence(long pullSequence) {
        this.pullSequence.set(pullSequence);
    }

    public long getAckSequence() {
        return ackSequence.get();
    }

    public void setAckSequence(long ackSequence) {
        this.ackSequence.set(ackSequence);
    }

    public void pullLock() {
        pullLock.lock();
    }

    public void pullUnlock() {
        pullLock.unlock();
    }

    public void ackLock() {
        ackLock.lock();
    }

    public void ackUnLock() {
        ackLock.unlock();
    }

    public boolean tryLock() {
        if (!pullLock.tryLock()) return false;
        if (!ackLock.tryLock()) {
            pullLock.unlock();
            return false;
        }
        return true;
    }

    public void unlock() {
        pullLock.unlock();
        ackLock.unlock();
    }

    @Override
    public String toString() {
        return "ConsumerSequence{" +
                "pullSequence=" + pullSequence +
                ", ackSequence=" + ackSequence +
                '}';
    }
}
