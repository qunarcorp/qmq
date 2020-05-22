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

import com.google.common.collect.ImmutableMap;
import qunar.tc.qmq.store.action.ForeverOfflineActionReaderWriter;
import qunar.tc.qmq.store.action.PullActionReaderWriter;
import qunar.tc.qmq.store.action.RangeAckActionReaderWriter;

import java.util.HashMap;
import java.util.Map;

/**
 * @author keli.wang
 * @since 2017/8/20
 */
public enum ActionType {
    PULL((byte) 0, new PullActionReaderWriter()),
    RANGE_ACK((byte) 1, new RangeAckActionReaderWriter()),
    FOREVER_OFFLINE((byte) 2, new ForeverOfflineActionReaderWriter());

    private static final ImmutableMap<Byte, ActionType> INSTANCES;

    static {
        final Map<Byte, ActionType> instances = new HashMap<>();
        for (final ActionType t : values()) {
            instances.put(t.getCode(), t);
        }
        INSTANCES = ImmutableMap.copyOf(instances);
    }

    private final byte code;
    private final ActionReaderWriter readerWriter;

    ActionType(final byte code, final ActionReaderWriter readerWriter) {
        this.code = code;
        this.readerWriter = readerWriter;
    }

    public static ActionType fromCode(final byte code) {
        if (INSTANCES.containsKey(code)) {
            return INSTANCES.get(code);
        }

        throw new RuntimeException("unknown action type code " + code);
    }

    public byte getCode() {
        return code;
    }

    public ActionReaderWriter getReaderWriter() {
        return readerWriter;
    }
}
