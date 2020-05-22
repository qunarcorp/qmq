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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author yunfeng.yang
 * @since 2017/8/1
 */
public class ActionLogOffsetResponse {
    private AtomicLong pullLogOffset;
    private AtomicLong ackLogOffset;
    private String brokerAddress;

    private Lock pullLock = new ReentrantLock();
    private Lock ackLock = new ReentrantLock();

    public ActionLogOffsetResponse(final long pullLogOffset, final long ackLogOffset) {
        this.pullLogOffset = new AtomicLong(pullLogOffset);
        this.ackLogOffset = new AtomicLong(ackLogOffset);
    }

    public long getPullLogOffset() {
        return pullLogOffset.get();
    }

    public void setPullLogOffset(long pullLogOffset) {
        this.pullLogOffset.set(pullLogOffset);
    }

    public long getAckLogOffset() {
        return ackLogOffset.get();
    }

    public void setAckLogOffset(long ackLogOffset) {
        this.ackLogOffset.set(ackLogOffset);
    }

    public void pullLock() {
        pullLock.lock();
    }

    public boolean tryPullLock() {
        return pullLock.tryLock();
    }

    public void pullUnlock() {
        pullLock.unlock();
    }

    public void ackLock() {
        ackLock.lock();
    }

    public boolean tryAckLock() {
        return ackLock.tryLock();
    }

    public void ackUnLock() {
        ackLock.unlock();
    }

    public String getBrokerAddress() {
        return brokerAddress;
    }

    public void setBrokerAddress(String brokerAddress) {
        this.brokerAddress = brokerAddress;
    }

    @Override
    public String toString() {
        return "ActionLogOffsetResponse{" +
                "pullLogOffset=" + pullLogOffset +
                ", ackLogOffset=" + ackLogOffset +
                ", brokerAddress='" + brokerAddress + '\'' +
                '}';
    }

    @Override
    public int hashCode() {

        return Objects.hash(pullLogOffset, ackLogOffset, brokerAddress);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ActionLogOffsetResponse that = (ActionLogOffsetResponse) o;

        if (!pullLogOffset.equals(that.pullLogOffset)) {
            return false;
        }
        if (!ackLogOffset.equals(that.ackLogOffset)) {
            return false;
        }
        return brokerAddress.equals(that.brokerAddress);
    }

}
