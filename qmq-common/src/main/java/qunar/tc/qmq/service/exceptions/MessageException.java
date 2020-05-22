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
package qunar.tc.qmq.service.exceptions;

/**
 * @author miao.yang susing@gmail.com
 * @date 2013-1-6
 */
public class MessageException extends Exception {

    public static final String BROKER_BUSY = "broker busy";
    public static final String REJECT_MESSAGE = "message rejected";
    public static final String UNKONW_MESSAGE = "unkonwn exception";

    private static final long serialVersionUID = -8385014158365588186L;

    private final String messageId;

    public MessageException(String messageId, String msg, Throwable t) {
        super(msg, t);
        this.messageId = messageId;
    }

    public MessageException(String messageId, String msg) {
        this(messageId, msg, null);
    }

    public String getMessageId() {
        return messageId;
    }

    @Override
    public Throwable initCause(Throwable cause) {
        return this;
    }

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }

    public boolean isBrokerBusy() {
        return BROKER_BUSY.equals(getMessage());
    }

    public boolean isSubjectNotAssigned() {
        return false;
    }

    public boolean isRejected() {
        return REJECT_MESSAGE.equals(getMessage());
    }
}
