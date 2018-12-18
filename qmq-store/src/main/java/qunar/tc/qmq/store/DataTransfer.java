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

import io.netty.channel.FileRegion;
import io.netty.util.ReferenceCounted;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * @author keli.wang
 * @since 2017/7/6
 */
public class DataTransfer implements FileRegion {
    private final ByteBuffer headerBuffer;
    private final SegmentBuffer segmentBuffer;
    private final int bufferTotalSize;

    private final ByteBuffer[] buffers;

    private long transferred;

    public DataTransfer(ByteBuffer headerBuffer, SegmentBuffer segmentBuffer, int bufferTotalSize) {

        this.headerBuffer = headerBuffer;
        this.segmentBuffer = segmentBuffer;
        this.bufferTotalSize = bufferTotalSize;

        this.buffers = new ByteBuffer[2];
        this.buffers[0] = headerBuffer;
        this.buffers[1] = segmentBuffer.getBuffer();
    }

    @Override
    public long position() {
        long pos = 0;
        for (ByteBuffer buffer : this.buffers) {
            pos += buffer.position();
        }
        return pos;
    }

    @Override
    public long transfered() {
        return transferred;
    }

    @Override
    public long count() {
        return headerBuffer.limit() + bufferTotalSize;
    }

    @Override
    public long transferTo(WritableByteChannel target, long position) throws IOException {
        GatheringByteChannel channel = (GatheringByteChannel) target;
        long write = channel.write(this.buffers);
        transferred += write;
        return write;
    }


    @Override
    public int refCnt() {
        return 0;
    }

    @Override
    public ReferenceCounted retain() {
        return null;
    }

    @Override
    public ReferenceCounted retain(int increment) {
        return null;
    }

    @Override
    public boolean release() {
        segmentBuffer.release();
        return true;
    }

    @Override
    public boolean release(int decrement) {
        return release();
    }
}
