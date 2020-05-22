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

package qunar.tc.qmq.protocol.producer;

/**
 * @author yunfeng.yang
 * @since 2017/7/6
 */
public class SendResult {

    public static final SendResult OK = new SendResult(MessageProducerCode.SUCCESS, "");

    private final int code;
    private final String remark;

    public SendResult(int code, String remark) {
        this.code = code;
        this.remark = remark;
    }

    public int getCode() {
        return code;
    }

    public String getRemark() {
        return remark;
    }

    @Override
    public String toString() {
        return "SendResult{" +
                "code=" + code +
                ", remark='" + remark + '\'' +
                '}';
    }
}
