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

package qunar.tc.qmq.broker;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by zhaohui.yu
 * 5/31/18
 */
class CircuitBreaker {

    private final AtomicReference<State> state;

    CircuitBreaker() {
        state = new AtomicReference<State>(new Closed(this));
    }

    private void switchState(State current, State to) {
        state.compareAndSet(current, to);
    }

    private void switchState(State to) {
        state.set(to);
    }

    void markFailed() {
        state.get().markFailed();
    }

    void markSuccess() {
        state.get().markSuccess();
    }

    boolean isAvailable() {
        return state.get().isAvailable();
    }

    private interface State {

        void markFailed();

        void markSuccess();

        boolean isAvailable();
    }

    private static class Open implements State {

        private final CircuitBreaker circuitBreaker;

        private final long lastFailedTs;

        Open(CircuitBreaker circuitBreaker) {
            this.circuitBreaker = circuitBreaker;
            this.lastFailedTs = System.currentTimeMillis();
        }

        @Override
        public void markFailed() {

        }

        @Override
        public void markSuccess() {

        }

        @Override
        public boolean isAvailable() {
            if (System.currentTimeMillis() - lastFailedTs > 5000) {
                circuitBreaker.switchState(this, new HalfOpen(circuitBreaker));
                return true;
            }
            return false;
        }
    }

    private static class HalfOpen implements State {
        private final AtomicInteger successCount;
        private final CircuitBreaker circuitBreaker;

        private final int maxSuccessCount = 20;


        HalfOpen(CircuitBreaker circuitBreaker) {
            this.circuitBreaker = circuitBreaker;
            this.successCount = new AtomicInteger(0);
        }

        @Override
        public boolean isAvailable() {
            return true;
        }

        @Override
        public void markFailed() {
            successCount.set(0);
            circuitBreaker.switchState(this, new Open(circuitBreaker));
        }

        public void markSuccess() {
            if (successCount.incrementAndGet() >= maxSuccessCount) {
                circuitBreaker.switchState(new Closed(circuitBreaker));
            }
        }
    }

    private static class Closed implements State {

        private final AtomicInteger failed;
        private final CircuitBreaker circuitBreaker;
        private final int maxFailedCount = 100;

        Closed(CircuitBreaker circuitBreaker) {
            this.circuitBreaker = circuitBreaker;
            this.failed = new AtomicInteger(0);
        }

        @Override
        public void markFailed() {
            if (failed.incrementAndGet() >= maxFailedCount) {
                circuitBreaker.switchState(this, new Open(circuitBreaker));
            }
        }

        @Override
        public void markSuccess() {
            failed.set(0);
        }

        @Override
        public boolean isAvailable() {
            return true;
        }

    }
}
