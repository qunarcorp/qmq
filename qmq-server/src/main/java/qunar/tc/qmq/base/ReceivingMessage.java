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

import com.google.common.util.concurrent.SettableFuture;

/**
 * @author yunfeng.yang
 * @since 2017/8/8
 */
public class ReceivingMessage {
    private final RawMessage message;
    private final SettableFuture<ReceiveResult> promise;

    private final long receivedTime;

    public ReceivingMessage(RawMessage message, long receivedTime) {
        this.message = message;
        this.receivedTime = receivedTime;
        this.promise = SettableFuture.create();
    }

    public RawMessage getMessage() {
        return message;
    }

    public SettableFuture<ReceiveResult> promise() {
        return promise;
    }

    public long getReceivedTime() {
        return receivedTime;
    }

    public String getMessageId() {
        return message.getHeader().getMessageId();
    }

    public void done(ReceiveResult result) {
        promise.set(result);
    }

    public String getSubject() {
        return message.getHeader().getSubject();
    }

    public boolean isExpired() {
        return System.currentTimeMillis() > message.getHeader().getExpireTime();
    }

    public boolean isHigh() {
        return message.isHigh();
    }

}
