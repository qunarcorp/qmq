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
 * @since 2017/8/19
 */
public enum BrokerRole {
    MASTER(BrokerGroupKind.NORMAL, 0),
    SLAVE(BrokerGroupKind.NORMAL, 1),
    STANDBY(BrokerGroupKind.NORMAL, 2),
    DELAY(BrokerGroupKind.DELAY, 3),
    BACKUP(BrokerGroupKind.NORMAL, 4),
    DELAY_MASTER(BrokerGroupKind.DELAY, 5),
    DELAY_SLAVE(BrokerGroupKind.DELAY, 6),
    DELAY_BACKUP(BrokerGroupKind.DELAY, 7);

    private final BrokerGroupKind kind;
    private final int code;

    BrokerRole(final BrokerGroupKind kind, final int code) {
        this.kind = kind;
        this.code = code;
    }

    public static BrokerRole fromCode(int role) {
        for (BrokerRole value : BrokerRole.values()) {
            if (value.getCode() == role) {
                return value;
            }
        }

        throw new RuntimeException("Unknown broker role code " + role);
    }

    public BrokerGroupKind getKind() {
        return kind;
    }

    public int getCode() {
        return code;
    }
}
