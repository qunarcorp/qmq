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

package qunar.tc.qmq.netty.client;

import qunar.tc.qmq.protocol.Datagram;

import java.util.concurrent.atomic.AtomicBoolean;

public class ResponseFuture {
    private final int opaque;
    private final long timeout;
    private final Callback callback;
    private final long beginTime = System.currentTimeMillis();
    private volatile long sentTime = -1;
    private volatile long requestEndTime = -1;

    private volatile boolean sendOk = true;
    private volatile boolean isTimeout = false;
    private volatile Datagram response = null;
    private volatile Throwable cause;
    private final AtomicBoolean executeCallbackOnlyOnce = new AtomicBoolean(false);

    ResponseFuture(int opaque, long timeoutMs, Callback callback) {
        this.opaque = opaque;
        this.timeout = timeoutMs;
        this.callback = callback;
    }

    public int getOpaque() {
        return opaque;
    }

    public long getTimeout() {
        return timeout;
    }

    public long getBeginTime() {
        return beginTime;
    }

    public boolean isSendOk() {
        return sendOk;
    }

    void setSendOk(boolean sendOk) {
        this.sentTime = System.currentTimeMillis();
        this.sendOk = sendOk;
    }

    public long getRequestCostTime() {
        return Math.max(0, requestEndTime - beginTime);
    }

    public boolean isTimeout() {
        return isTimeout;
    }

    public Datagram getResponse() {
        return response;
    }

    public void completeBySendFail(Throwable cause) {
        setSendOk(false);
        this.requestEndTime = System.currentTimeMillis();
        this.cause = cause;
    }

    public void completeByTimeoutClean() {
        this.isTimeout = true;
        this.requestEndTime = System.currentTimeMillis();
    }

    public void completeByReceiveResponse(final Datagram response) {
        this.response = response;
        this.requestEndTime = System.currentTimeMillis();
    }

    public Throwable getCause() {
        return cause;
    }

    public void setCause(Throwable cause) {
        this.cause = cause;
    }

    void executeCallbackOnlyOnce() {
        if (callback == null) return;

        if (this.executeCallbackOnlyOnce.compareAndSet(false, true)) {
            callback.processResponse(this);
        }
    }

    @Override
    public String toString() {
        return "ResponseFuture{"
                + "opaque=" + opaque + ", "
                + "begin=" + beginTime + ", "
                + "send=" + sentTime + ", "
                + "end=" + requestEndTime + ", "
                + "response=" + response + ", "
                + "sendOk=" + sendOk + ", "
                + "isTimeout=" + isTimeout + ", "
                + (isTimeout ? "timeout=" + timeout + ", " : "")
                + "cause=" + cause
                + "}";
    }

    public interface Callback {
        void processResponse(ResponseFuture responseFuture);
    }
}
