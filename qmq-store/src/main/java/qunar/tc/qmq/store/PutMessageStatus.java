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

package qunar.tc.qmq.store;

/**
 * @author keli.wang
 * @since 2017/7/5
 */
public enum PutMessageStatus {
    SUCCESS(0),
    CREATE_MAPPED_FILE_FAILED(1),
    MESSAGE_ILLEGAL(2),
    ALREADY_WRITTEN(3),
    UNKNOWN_ERROR(-1);

    private int code;

    PutMessageStatus(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}