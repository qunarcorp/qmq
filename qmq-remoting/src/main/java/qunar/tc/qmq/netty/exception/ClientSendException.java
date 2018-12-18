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
 * @author yiqun.fan create on 17-7-5.
 */
public class ClientSendException extends Exception {

    private static final long serialVersionUID = 6006709158339785244L;

    private final SendErrorCode sendErrorCode;
    private final String brokerAddr;

    public ClientSendException(SendErrorCode sendErrorCode) {
        super(sendErrorCode.name());
        this.sendErrorCode = sendErrorCode;
        this.brokerAddr = "";
    }

    public ClientSendException(SendErrorCode sendErrorCode, String brokerAddr) {
        super(sendErrorCode.name() + ", broker address [" + brokerAddr + "]");
        this.sendErrorCode = sendErrorCode;
        this.brokerAddr = brokerAddr;
    }

    public ClientSendException(SendErrorCode sendErrorCode, String brokerAddr, Throwable cause) {
        super(sendErrorCode.name() + ", broker address [" + brokerAddr + "]", cause);
        this.sendErrorCode = sendErrorCode;
        this.brokerAddr = brokerAddr;
    }

    public SendErrorCode getSendErrorCode() {
        return sendErrorCode;
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }

    public enum SendErrorCode {
        ILLEGAL_OPAQUE,
        EMPTY_ADDRESS,
        CREATE_CHANNEL_FAIL,
        CONNECT_BROKER_FAIL,
        WRITE_CHANNEL_FAIL,
        BROKER_BUSY
    }
}
