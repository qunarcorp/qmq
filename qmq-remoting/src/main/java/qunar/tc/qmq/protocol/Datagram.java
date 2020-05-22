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

package qunar.tc.qmq.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;

/**
 * @author yiqun.fan create on 17-7-4.
 */
public class Datagram {
    RemotingHeader header;
    private ByteBuf body;
    private PayloadHolder holder;

    public ByteBuf getBody() {
        return body;
    }

    public void setBody(ByteBuf body) {
        this.body = body;
    }

    public void setPayloadHolder(PayloadHolder holder) {
        this.holder = holder;
    }

    public RemotingHeader getHeader() {
        return header;
    }

    public void setHeader(RemotingHeader header) {
        this.header = header;
    }

    public void writeBody(ByteBuf out) {
        if (holder == null) return;
        holder.writeBody(out);
    }

    public void release() {
        ReferenceCountUtil.safeRelease(body);
    }

    @Override
    public String toString() {
        return "Datagram{" +
                "header=" + header +
                '}';
    }
}
