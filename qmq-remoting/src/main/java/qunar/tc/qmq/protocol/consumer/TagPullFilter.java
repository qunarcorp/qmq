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

import qunar.tc.qmq.TagType;
import qunar.tc.qmq.utils.CharsetUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author keli.wang
 * @since 2019-01-02
 */
public class TagPullFilter implements PullFilter {
    private final int tagTypeCode;
    private final List<byte[]> tags;

    public TagPullFilter(final int tagTypeCode, final List<byte[]> tags) {
        this.tagTypeCode = tagTypeCode;
        this.tags = tags;
    }

    public TagPullFilter(final TagType tagType, final Set<String> tags) {
        this.tagTypeCode = tagType.getCode();
        this.tags = toBytes(tags);
    }

    private List<byte[]> toBytes(final Set<String> tags) {
        final List<byte[]> bytesTags = new ArrayList<>(tags.size());
        for (final String tag : tags) {
            bytesTags.add(tag.getBytes(CharsetUtils.UTF8));
        }
        return bytesTags;
    }

    public int getTagTypeCode() {
        return tagTypeCode;
    }

    public List<byte[]> getTags() {
        return tags;
    }

    @Override
    public PullFilterType type() {
        return PullFilterType.TAG;
    }
}
