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

package qunar.tc.qmq.stats;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author keli.wang
 * @since 2018/7/19
 */
public class PerMinuteDeltaCounter {
    private final AtomicLong value;
    private final long resetIntervalMillis;
    private final AtomicLong nextResetTimeMillisRef;

    public PerMinuteDeltaCounter() {
        this.value = new AtomicLong();
        this.resetIntervalMillis = TimeUnit.MINUTES.toMillis(1);
        this.nextResetTimeMillisRef = new AtomicLong(now() + resetIntervalMillis);
    }

    private long now() {
        return System.currentTimeMillis();
    }

    public void add(final long delta) {
        while (true) {
            final long nextResetTimeMillis = nextResetTimeMillisRef.get();
            final long currentTimeMillis = now();
            if (currentTimeMillis < nextResetTimeMillis) {
                value.addAndGet(delta);
                return;
            }
            final long currentValue = value.get();
            if (nextResetTimeMillisRef.compareAndSet(nextResetTimeMillis, Long.MAX_VALUE)) {
                value.addAndGet(delta - currentValue);
                nextResetTimeMillisRef.set(currentTimeMillis + resetIntervalMillis);
                return;
            }
        }
    }

    public long getSum() {
        while (true) {
            final long nextResetTimeMillis = nextResetTimeMillisRef.get();
            final long currentValue = value.get();
            final long currentTimeMillis = now();
            if (currentTimeMillis < nextResetTimeMillis) {
                return currentValue;
            }

            if (nextResetTimeMillisRef.compareAndSet(nextResetTimeMillis, Long.MAX_VALUE)) {
                value.addAndGet(-currentValue);
                nextResetTimeMillisRef.set(currentTimeMillis + resetIntervalMillis);
                return value.get();
            }
        }
    }
}
