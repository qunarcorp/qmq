/*
 * Copyright 2019 Qunar, Inc.
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

package qunar.tc.qmq.store.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCountUtil;

import java.nio.ByteBuffer;

/**
 * @author keli.wang
 * @since 2019-06-10
 */
public class MemTableBuffer implements Buffer {
    private final ByteBuf buf;
    private final int size;

    public MemTableBuffer(final ByteBuf buf, final int size) {
        this.buf = buf;
        this.size = size;
    }

    @Override
    public ByteBuffer getBuffer() {
        return buf.nioBuffer();
    }

    @Override
    public int getSize() {
        return size;
    }

    @Override
    public boolean retain() {
        try {
            buf.retain();
            return true;
        } catch (IllegalReferenceCountException ignore) {
            return false;
        }
    }

    @Override
    public boolean release() {
        ReferenceCountUtil.safeRelease(buf);
        return true;
    }
}
