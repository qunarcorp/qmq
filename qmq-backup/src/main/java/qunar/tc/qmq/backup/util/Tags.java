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

package qunar.tc.qmq.backup.util;

import com.google.common.collect.Sets;
import qunar.tc.qmq.utils.Flags;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Set;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-11-29 10:40
 */
public class Tags {

    public static Set<String> readTags(byte flag, ByteBuffer buffer) {
        //noinspection Duplicates
        if (Flags.hasTags(flag)) {
            final byte tagsSize = buffer.get();
            Set<String> tags = Sets.newHashSetWithExpectedSize(tagsSize);
            for (int i = 0; i < tagsSize; i++) {
                final String tag = PayloadHolderUtils.readString(buffer);
                tags.add(tag);
            }
            return tags;

        }
        return Collections.emptySet();
    }
}
