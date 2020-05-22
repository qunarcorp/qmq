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

package qunar.tc.qmq.delay.base;

import com.google.common.util.concurrent.SettableFuture;
import qunar.tc.qmq.delay.store.model.RawMessageExtend;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-07-26 14:17
 */
public class ReceivedDelayMessage {
    private final RawMessageExtend message;
    private final SettableFuture<ReceivedResult> promise;
    private final long receivedTime;

    public ReceivedDelayMessage(RawMessageExtend message, long receivedTime) {
        this.message = message;
        this.receivedTime = receivedTime;
        this.promise = SettableFuture.create();
    }

    public RawMessageExtend getMessage() {
        return message;
    }

    public void done(ReceivedResult result) {
        promise.set(result);
    }

    public String getMessageId() {
        return message.getHeader().getMessageId();
    }

    public String getSubject() {
        return message.getHeader().getSubject();
    }

    public SettableFuture<ReceivedResult> getPromise() {
        return promise;
    }

    public long getReceivedTime() {
        return receivedTime;
    }

    public boolean isExpired() {
        return System.currentTimeMillis() > message.getHeader().getExpireTime();
    }

    public long getScheduleTime() {
        return message.getScheduleTime();
    }

    public void adjustScheduleTime(long scheduleTime) {
        message.setScheduleTime(scheduleTime);
    }

    @Override
    public String toString() {
        return "ReceivedDelayMessage{" +
                "message=" + message +
                ", promise=" + promise +
                ", receivedTime=" + receivedTime +
                '}';
    }
}
