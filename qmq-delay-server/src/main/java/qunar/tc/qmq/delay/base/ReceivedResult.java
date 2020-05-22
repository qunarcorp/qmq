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

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-07-26 14:19
 */
public class ReceivedResult {
    private final String messageId;
    private final int code;
    private final String remark;
    private final long messageLogOffset;

    public ReceivedResult(String messageId, int code, String remark) {
        this(messageId, code, remark, -1);
    }

    public ReceivedResult(String messageId, int code, String remark, long messageLogOffset) {
        this.messageId = messageId;
        this.code = code;
        this.remark = remark;
        this.messageLogOffset = messageLogOffset;
    }

    public String getMessageId() {
        return messageId;
    }

    public int getCode() {
        return code;
    }

    public String getRemark() {
        return remark;
    }

    public long getMessageLogOffset() {
        return messageLogOffset;
    }
}
