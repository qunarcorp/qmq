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

package qunar.tc.qmq.protocol.consumer;

/**
 * @author keli.wang
 * @since 2019-01-02
 */
public enum PullFilterType {
    TAG((short) 1),
    SUB_ENV_ISOLATION((short) 2);

    private final short code;

    PullFilterType(final short code) {
        this.code = code;
    }

    public static PullFilterType fromCode(final short code) {
        for (final PullFilterType type : values()) {
            if (type.getCode() == code) {
                return type;
            }
        }

        throw new RuntimeException("unknown pull filter type code " + code);
    }

    public short getCode() {
        return code;
    }
}
