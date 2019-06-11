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

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.TagType;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static qunar.tc.qmq.protocol.RemotingHeader.VERSION_8;
import static qunar.tc.qmq.protocol.RemotingHeader.VERSION_9;

/**
 * @author keli.wang
 * @since 2019-01-02
 */
public class PullRequestSerde {
    public void write(final PullRequest request, final ByteBuf out) {
        PayloadHolderUtils.writeString(request.getSubject(), out);
        PayloadHolderUtils.writeString(request.getGroup(), out);
        PayloadHolderUtils.writeString(request.getConsumerId(), out);

        out.writeInt(request.getRequestNum());
        out.writeLong(request.getOffset());
        out.writeLong(request.getPullOffsetBegin());
        out.writeLong(request.getPullOffsetLast());
        out.writeLong(request.getTimeoutMillis());
        out.writeByte(request.isBroadcast() ? 1 : 0);

        writeFilters(request.getFilters(), out);
    }

    private void writeFilters(final List<PullFilter> filters, final ByteBuf out) {
        if (filters == null || filters.isEmpty()) {
            out.writeInt(0);
            return;
        }

        out.writeInt(filters.size());
        for (final PullFilter filter : filters) {
            writeFilter(filter, out);
        }
    }

    private void writeFilter(final PullFilter filter, final ByteBuf out) {
        final PullFilterType type = filter.type();

        out.writeShort(type.getCode());
        switch (type) {
            case TAG:
                writeTagPullFilter((TagPullFilter) filter, out);
                break;
            case SUB_ENV_ISOLATION:
                writeSubEnvMatchPullFilter((SubEnvIsolationPullFilter) filter, out);
                break;
            default:
                break;
        }
    }

    private void writeTagPullFilter(final TagPullFilter filter, final ByteBuf out) {
        out.writeShort(filter.getTagTypeCode());
        out.writeByte(filter.getTags().size());
        for (byte[] tag : filter.getTags()) {
            out.writeShort((short) tag.length);
            out.writeBytes(tag);
        }
    }

    private void writeSubEnvMatchPullFilter(final SubEnvIsolationPullFilter filter, final ByteBuf out) {
        PayloadHolderUtils.writeString(filter.getEnv(), out);
        PayloadHolderUtils.writeString(filter.getSubEnv(), out);
    }

    public PullRequest read(final int version, final ByteBuf in) {
        final String prefix = PayloadHolderUtils.readString(in);
        final String group = PayloadHolderUtils.readString(in);
        final String consumerId = PayloadHolderUtils.readString(in);
        final int requestNum = in.readInt();
        final long offset = in.readLong();
        final long pullOffsetBegin = in.readLong();
        final long pullOffsetLast = in.readLong();
        final long timeout = in.readLong();
        final byte broadcast = in.readByte();
        final List<PullFilter> filters = readFilters(version, in);

        final PullRequest request = new PullRequest();
        request.setSubject(prefix);
        request.setGroup(group);
        request.setConsumerId(consumerId);
        request.setRequestNum(requestNum);
        request.setOffset(offset);
        request.setPullOffsetBegin(pullOffsetBegin);
        request.setPullOffsetLast(pullOffsetLast);
        request.setTimeoutMillis(timeout);
        request.setBroadcast(broadcast != 0);
        request.setFilters(filters);
        return request;

    }

    private List<PullFilter> readFilters(final int version, final ByteBuf in) {
        if (version < VERSION_8) {
            return Collections.emptyList();
        }
        if (version < VERSION_9) {
            final PullFilter filter = readTagPullFilter(in);
            if (filter == null) {
                return Collections.emptyList();
            } else {
                return Collections.singletonList(filter);
            }
        }

        final int filterCount = in.readInt();
        if (filterCount <= 0) {
            return Collections.emptyList();
        }
        final List<PullFilter> filters = new ArrayList<>(filterCount);
        for (int i = 0; i < filterCount; i++) {
            final PullFilter filter = readFilter(in);
            if (filter != null) {
                filters.add(filter);
            }
        }
        return filters;
    }

    private PullFilter readFilter(final ByteBuf in) {
        final short typeCode = in.readShort();
        final PullFilterType type = PullFilterType.fromCode(typeCode);
        switch (type) {
            case TAG:
                return readTagPullFilter(in);
            case SUB_ENV_ISOLATION:
                return readSubEnvMatchPullFilter(in);
            default:
                throw new RuntimeException("unsupported pull filter type " + type);
        }
    }

    private PullFilter readTagPullFilter(final ByteBuf in) {
        final int tagTypeCode = in.readShort();
        final byte tagSize = in.readByte();
        final List<byte[]> tags = new ArrayList<>(tagSize);
        for (int i = 0; i < tagSize; i++) {
            final int len = in.readShort();
            final byte[] bs = new byte[len];
            in.readBytes(bs);
            tags.add(bs);
        }

        if (tagTypeCode == TagType.NO_TAG.getCode()) {
            return null;
        } else {
            return new TagPullFilter(tagTypeCode, tags);
        }
    }

    private PullFilter readSubEnvMatchPullFilter(final ByteBuf in) {
        final String env = PayloadHolderUtils.readString(in);
        final String subEnv = PayloadHolderUtils.readString(in);
        return new SubEnvIsolationPullFilter(env, subEnv);
    }
}
