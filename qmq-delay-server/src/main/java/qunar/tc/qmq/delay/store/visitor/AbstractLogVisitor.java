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

package qunar.tc.qmq.delay.store.visitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by zhaohui.yu
 * 8/20/18
 */
public abstract class AbstractLogVisitor<T> implements LogVisitor<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DispatchLogVisitor.class);

    private final FileChannel fileChannel;
    private final AtomicLong visited;
    private final AtomicLong visitedSnapshot;
    private final ByteBuffer buffer;

    AbstractLogVisitor(long from, FileChannel fileChannel, int workingSize) {
        this.fileChannel = fileChannel;
        this.visited = new AtomicLong(from);
        this.visitedSnapshot = new AtomicLong(from);
        buffer = ByteBuffer.allocateDirect(workingSize);
        try {
            fileChannel.read(buffer, visited.get());
            buffer.flip();
        } catch (IOException e) {
            LOGGER.error("load dispatch log visitor error", e);
        }
    }

    @Override
    public Optional<T> nextRecord() {
        int start = buffer.position();
        Optional<T> optional = readOneRecord(buffer);
        int delta = buffer.position() - start;

        if (optional.isPresent()) visited.addAndGet(delta);
        else if (visited.get() > visitedSnapshot.get()) {
            if (reAlloc()) return nextRecord();
        }
        return optional;
    }

    private boolean reAlloc() {
        try {
            buffer.clear();
            int bytes = fileChannel.read(buffer, visited.get());
            if (bytes > 0) {
                buffer.flip();
                visitedSnapshot.addAndGet(visited.get() - visitedSnapshot.get());
                return true;
            }
        } catch (IOException e) {
            LOGGER.error("load visitor nextRecord error", e);
        }

        return false;
    }

    protected abstract Optional<T> readOneRecord(ByteBuffer buffer);

    @Override
    public long visitedBufferSize() {
        return visited.get();
    }

    @Override
    public void close() {
        if (buffer == null) return;
        Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
        cleaner.clean();
    }
}
