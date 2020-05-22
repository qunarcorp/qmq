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

package qunar.tc.qmq.consumer.pull;

import com.google.common.util.concurrent.AbstractFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.Message;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author yiqun.fan create on 17-10-19.
 */
class PullMessageFuture extends AbstractFuture<List<Message>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(PullMessageFuture.class);
    private final int expectedSize;
    private final int fetchSize;
    private final long timeout;
    private final boolean isResetCreateTime;
    private volatile long createTime = System.currentTimeMillis();

    public PullMessageFuture(int expectedSize, int fetchSize, long timeout, boolean isResetCreateTime) {
        this.expectedSize = expectedSize;
        this.fetchSize = fetchSize;
        this.timeout = timeout;
        this.isResetCreateTime = isResetCreateTime;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public int getExpectedSize() {
        return expectedSize;
    }

    public boolean isResetCreateTime() {
        return isResetCreateTime;
    }

    public void resetCreateTime() {
        createTime = System.currentTimeMillis();
    }

    public boolean isExpired() {
        return System.currentTimeMillis() - timeout > createTime;
    }

    public boolean isPullOnce() {
        return timeout < 0;
    }

    public int getTimeout() {
        return (int) timeout;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean set(List<Message> value) {
        return super.set(value);
    }

    @Override
    public List<Message> get() {
        while (true) {
            try {
                return super.get();
            } catch (InterruptedException e) {
                LOGGER.info("ignore interrupt pull");
            } catch (ExecutionException e) {
                return Collections.emptyList();
            }
        }
    }

    @Override
    public List<Message> get(long timeout, TimeUnit unit) throws TimeoutException, ExecutionException {
        long current = System.currentTimeMillis();
        long endTime = current + unit.toMillis(timeout);
        while (endTime > current) {
            try {
                return super.get(endTime - current, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                LOGGER.info("ignore interrupt pull");
            }
            current = System.currentTimeMillis();
        }
        throw new TimeoutException();
    }
}
