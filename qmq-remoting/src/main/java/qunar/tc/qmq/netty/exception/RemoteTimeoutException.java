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

package qunar.tc.qmq.netty.exception;

/**
 * @author zhenyu.nie created on 2017 2017/7/6 16:44
 */
public class RemoteTimeoutException extends RemoteException {

    private static final long serialVersionUID = -7427009787627990391L;

    public RemoteTimeoutException() {
    }

    public RemoteTimeoutException(Throwable cause) {
        super(cause);
    }

    public RemoteTimeoutException(String address, long timeoutMs) {
        this(address, timeoutMs, null);
    }

    public RemoteTimeoutException(String address, long timeoutMs, Throwable cause) {
        super("remote timeout on address [" + address + "] with timeout [" + timeoutMs + "]ms", cause);
    }
}
