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
import qunar.tc.qmq.protocol.PayloadHolder;
import qunar.tc.qmq.utils.PayloadHolderUtils;

/**
 * @author yiqun.fan create on 17-8-2.
 */
public class PullRequestPayloadHolder implements PayloadHolder {
    private final PullRequest request;

    public PullRequestPayloadHolder(PullRequest request) {
        this.request = request;
    }

    @Override
    public void writeBody(ByteBuf out) {
        PayloadHolderUtils.writeString(request.getSubject(), out);
        PayloadHolderUtils.writeString(request.getGroup(), out);
        PayloadHolderUtils.writeString(request.getConsumerId(), out);
        out.writeInt(request.getRequestNum());
        out.writeLong(request.getOffset());
        out.writeLong(request.getPullOffsetBegin());
        out.writeLong(request.getPullOffsetLast());
        out.writeLong(request.getTimeoutMillis());
        out.writeByte(request.isBroadcast() ? 1 : 0);
        out.writeShort(request.getTagTypeCode());
        out.writeByte(request.getTags().size());
        for (byte[] tag : request.getTags()) {
            out.writeShort((short) tag.length);
            out.writeBytes(tag);
        }
    }
}
