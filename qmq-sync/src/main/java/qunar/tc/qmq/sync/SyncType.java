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

package qunar.tc.qmq.sync;

/**
 * @author yunfeng.yang
 * @since 2017/8/18
 */
public enum SyncType {
    action(1), message(2), heartbeat(3), dispatch(4), index(5);

    private int code;

    SyncType(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
