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

package qunar.tc.qmq.delay.store.model;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-07-11 21:21
 */
public enum MessageLogAttrEnum {
    /**
     * 正常记录
     */
    ATTR_MESSAGE_RECORD((byte) 0),
    /**
     * 空记录
     */
    ATTR_EMPTY_RECORD((byte) 1),
    /**
     * 需要跳过记录
     */
    ATTR_SKIP_RECORD((byte) 2);

    private byte code;

    MessageLogAttrEnum(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }


}
