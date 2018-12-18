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

package qunar.tc.qmq.meta;

/**
 * @author yunfeng.yang
 * @since 2017/8/28
 */
public enum BrokerState {
    RW(1), R(2), W(3), NRW(4);

    private final int code;

    BrokerState(int code) {
        this.code = code;
    }

    public static BrokerState codeOf(int brokerState) {
        for (BrokerState value : BrokerState.values()) {
            if (value.getCode() == brokerState) {
                return value;
            }
        }
        return null;
    }

    public int getCode() {
        return code;
    }

    public boolean canRead() {
        return code == RW.code || code == R.code;
    }

    public boolean canWrite() {
        return code == RW.code || code == W.code;
    }
}
