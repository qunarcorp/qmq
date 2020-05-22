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

package qunar.tc.qmq.base;

/**
 * yiqun.fan@qunar.com 2018/3/2
 */
public enum OnOfflineState {

    ONLINE(0), OFFLINE(1);

    private final int code;

    OnOfflineState(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    @Override
    public String toString() {
        return code == 0 ? "ON" : "OFF";
    }

    public static OnOfflineState fromCode(int code) {
        return code == 0 ? ONLINE : OFFLINE;
    }
}
