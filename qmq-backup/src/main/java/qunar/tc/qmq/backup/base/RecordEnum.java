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

package qunar.tc.qmq.backup.base;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019/5/29
 */
public enum RecordEnum {
    RECORD((byte) 0),
    RETRY_RECORD((byte) 1),
    DEAD_RECORD((byte) 2),
    OTHER((byte) -1);

    private byte code;

    RecordEnum(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }
}
