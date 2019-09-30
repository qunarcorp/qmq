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

package qunar.tc.qmq;

/**
 * Created by zhaohui.yu
 * 4/9/18
 */
public enum StatusSource {
    // 0001 0010 0100
    HEALTHCHECKER((byte) 1),
    OPS((byte) 2),
    CODE((byte) 4);

    private byte code;

    StatusSource(byte code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
