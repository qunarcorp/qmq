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

import java.util.HashMap;
import java.util.Map;

/**
 * @author yunfeng.yang
 * @since 2018/1/18
 */
public enum TagType {
    NO_TAG(0), OR(1), AND(2);

    private int code;

    TagType(int code) {
        this.code = code;
    }

    private final static Map<Integer, TagType> MAP = new HashMap<>();

    static {
        for (TagType tagType : TagType.values()) {
            MAP.put(tagType.getCode(), tagType);
        }
    }

    public static TagType of(final int code) {
        final TagType tagType = MAP.get(code);
        if (tagType != null) {
            return tagType;
        }
        return TagType.NO_TAG;
    }

    public int getCode() {
        return code;
    }
}
