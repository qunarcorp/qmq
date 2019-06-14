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

import qunar.tc.qmq.TagType;
import qunar.tc.qmq.store.buffer.Buffer;
import qunar.tc.qmq.utils.Flags;
import qunar.tc.qmq.utils.ListUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

class Tags {
    static boolean match(Buffer result, List<byte[]> requestTags, int tagTypeCode) {
        ByteBuffer message = result.getBuffer();
        message.mark();
        byte flag = message.get();
        if (!Flags.hasTags(flag)) {
            message.reset();
            return false;
        }
        skip(message, 8 + 8);
        //subject
        skipString(message);
        //message id
        skipString(message);

        final byte tagsSize = message.get();
        if (tagsSize == 1) {
            final short len = message.getShort();
            final byte[] tag = new byte[len];
            message.get(tag);
            message.reset();
            return matchOneTag(tag, requestTags, tagTypeCode);
        }
        List<byte[]> tags = new ArrayList<>(tagsSize);
        for (int i = 0; i < tagsSize; i++) {
            final short len = message.getShort();
            final byte[] bs = new byte[len];
            message.get(bs);
            tags.add(bs);
        }
        message.reset();
        return matchTags(tags, requestTags, tagTypeCode);
    }

    private static void skipString(ByteBuffer input) {
        final short len = input.getShort();
        skip(input, len);
    }

    private static void skip(ByteBuffer input, int bytes) {
        input.position(input.position() + bytes);
    }

    private static boolean matchOneTag(byte[] tag, List<byte[]> requestTags, int tagTypeCode) {
        if (requestTags.size() == 1 || TagType.OR.getCode() == tagTypeCode) {
            return ListUtils.contains(requestTags, tag);
        }

        if (TagType.AND.getCode() == tagTypeCode && requestTags.size() > 1) return false;

        return false;
    }

    private static boolean matchTags(List<byte[]> messageTags, List<byte[]> requestTags, int tagTypeCode) {
        if (tagTypeCode == TagType.AND.getCode()) {
            return ListUtils.containsAll(messageTags, requestTags);
        }
        if (tagTypeCode == TagType.OR.getCode()) {
            return ListUtils.intersection(messageTags, requestTags);
        }
        return false;
    }
}
