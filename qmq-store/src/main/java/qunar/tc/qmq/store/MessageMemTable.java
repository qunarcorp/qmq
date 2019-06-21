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

package qunar.tc.qmq.store;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.store.buffer.MemTableBuffer;
import qunar.tc.qmq.store.result.Result;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author keli.wang
 * @since 2019-06-10
 */
public class MessageMemTable implements Iterable<MessageMemTable.Entry>, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(MessageMemTable.class);

    // write extra sequence before each message
    private static final int OVERHEAD_BYTES = Long.BYTES;

    private final Map<String, Long> firstSequences;
    private final Map<String, ArrayList<MessageIndex>> indexes;
    private final ByteBuf mem;
    private final ReadWriteLock rwLock;

    private final long tabletId;
    private final long beginOffset;
    private volatile long endOffset;

    public MessageMemTable(final long tabletId, final long beginOffset, final int capacity) {
        this.firstSequences = new HashMap<>();
        this.indexes = new HashMap<>();
        this.mem = ByteBufAllocator.DEFAULT.ioBuffer(capacity);
        this.rwLock = new ReentrantReadWriteLock();

        this.tabletId = tabletId;
        this.beginOffset = beginOffset;
    }

    public int getOverheadBytes() {
        return OVERHEAD_BYTES;
    }

    public long getTabletId() {
        return tabletId;
    }

    public long getBeginOffset() {
        return beginOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public Map<String, Long> getFirstSequences() {
        return Collections.unmodifiableMap(firstSequences);
    }

    public Map<String, Long> getNextSequences() {
        final Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            final HashMap<String, Long> nextSequences = new HashMap<>(firstSequences.size());
            firstSequences.forEach((subject, firstSequence) -> {
                final ArrayList<MessageIndex> indexes = this.indexes.get(subject);
                nextSequences.put(subject, firstSequence + indexes.size());
            });
            return nextSequences;
        } finally {
            lock.unlock();
        }
    }

    public int getTotalDataSize() {
        return mem.readableBytes();
    }

    public boolean checkWritable(final int writeBytes) {
        return mem.writableBytes() - getOverheadBytes() > writeBytes;
    }

    public Result<GetResultStatus, MemTableBuffer> get(final String subject, final long sequence) {
        final Lock lock = rwLock.readLock();
        lock.lock();
        try {
            final Long firstSequence = firstSequences.get(subject);
            if (firstSequence == null) {
                return new Result<>(GetResultStatus.SUBJECT_NOT_FOUND, null);
            }

            if (sequence < firstSequence) {
                return new Result<>(GetResultStatus.SEQUENCE_TOO_SMALL, null);
            }

            final int relativeIndex = (int) (sequence - firstSequence);
            final ArrayList<MessageIndex> subjectIndexes = indexes.get(subject);
            if (relativeIndex < 0 || subjectIndexes == null || relativeIndex >= subjectIndexes.size()) {
                return new Result<>(GetResultStatus.SEQUENCE_TOO_LARGE, null);
            }

            final MessageIndex index = subjectIndexes.get(relativeIndex);
            final MemTableBuffer buffer = selectMessageBuffer(index);
            if (buffer.retain()) {
                return new Result<>(GetResultStatus.SUCCESS, buffer);
            } else {
                return new Result<>(GetResultStatus.TABLE_ALREADY_EVICTED, null);
            }
        } finally {
            lock.unlock();
        }
    }

    public GetMessageResult poll(final String subject, final long beginSequence,
                                 final int maxMessages, final MessageFilter filter) {
        final Lock lock = rwLock.readLock();
        lock.lock();
        try {
            final GetMessageResult result = new GetMessageResult();

            final Long firstSequence = firstSequences.get(subject);
            if (firstSequence == null) {
                result.setNextBeginSequence(0);
                result.setStatus(GetMessageStatus.SUBJECT_NOT_FOUND);
                return result;
            }

            if (beginSequence < firstSequence) {
                result.setNextBeginSequence(0);
                result.setStatus(GetMessageStatus.SEQUENCE_TOO_SMALL);
                return result;
            }

            final int beginRelativeIndex = (int) (beginSequence - firstSequence);
            final ArrayList<MessageIndex> subjectIndexes = indexes.get(subject);
            if (beginRelativeIndex < 0 || subjectIndexes == null || beginRelativeIndex > subjectIndexes.size()) {
                result.setNextBeginSequence(beginSequence);
                result.setStatus(GetMessageStatus.SEQUENCE_TOO_LARGE);
                return result;
            }

            if (beginRelativeIndex == subjectIndexes.size()) {
                result.setNextBeginSequence(beginSequence);
                result.setStatus(GetMessageStatus.NO_MESSAGE);
                return result;
            }

            int relativeIndex = beginRelativeIndex;
            while (relativeIndex < subjectIndexes.size() && result.getBuffers().size() < maxMessages) {
                final MessageIndex index = subjectIndexes.get(relativeIndex);
                if (!filter.filter(index)) {
                    break;
                }
                final MemTableBuffer buffer = selectMessageBuffer(index);
                if (!buffer.retain()) {
                    result.setNextBeginSequence(firstSequence + relativeIndex);
                    result.setStatus(GetMessageStatus.TABLE_ALREADY_EVICTED);
                    return result;
                }
                result.addBuffer(buffer);
                relativeIndex++;
            }

            final long nextBeginSequence = firstSequence + relativeIndex;
            result.setNextBeginSequence(nextBeginSequence);
            result.setConsumerLogRange(new OffsetRange(beginSequence, nextBeginSequence - 1));
            result.setStatus(GetMessageStatus.SUCCESS);
            return result;
        } finally {
            lock.unlock();
        }
    }

    private MemTableBuffer selectMessageBuffer(final MessageIndex index) {
        return new MemTableBuffer(mem.slice(index.position, index.size), index.size);
    }

    private MemTableBuffer selectRecordBuffer(final MessageIndex index) {
        final int position = index.position - getOverheadBytes();
        final int size = index.size + getOverheadBytes();
        return new MemTableBuffer(mem.slice(position, size), size);
    }

    public Result<AddResultStatus, MessageIndex> add(final String subject, final long sequence, final long offset, final ByteBuffer message) {
        final Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            final Long firstSequence = firstSequences.get(subject);
            if (firstSequence != null) {
                if (sequence < firstSequence) {
                    LOG.error("sequence reverted. subject: {}, firstSequence: {}, sequence: {}", subject, firstSequence, sequence);
                    return new Result<>(AddResultStatus.SUCCESS, null);
                }
                final int index = (int) (sequence - firstSequence);
                final ArrayList<MessageIndex> subjectIndexes = this.indexes.computeIfAbsent(subject, (s) -> new ArrayList<>());
                if (index < subjectIndexes.size()) {
                    return new Result<>(AddResultStatus.SUCCESS, subjectIndexes.get(index));
                }
            }

            final Result<AddResultStatus, MessageIndex> result = append(sequence, message);
            if (result.getStatus() == AddResultStatus.SUCCESS) {
                endOffset = offset;
                firstSequences.putIfAbsent(subject, sequence);
                indexes.computeIfAbsent(subject, (s) -> new ArrayList<>()).add(result.getData());
            }
            return result;
        } finally {
            lock.unlock();
        }
    }

    private Result<AddResultStatus, MessageIndex> append(final long sequence, final ByteBuffer message) {
        final int size = message.limit();
        if (mem.writableBytes() < size + OVERHEAD_BYTES) {
            return new Result<>(AddResultStatus.OVERFLOW, null);
        }

        mem.writeLong(sequence);
        final int position = mem.writerIndex();
        mem.writeBytes(message);
        return new Result<>(AddResultStatus.SUCCESS, new MessageIndex(System.currentTimeMillis(), position, size));
    }

    /**
     * Don't iterate when adding new messages into this mem table
     */
    @Override
    public Iterator<Entry> iterator() {
        return new TableIterator(this);
    }

    @Override
    public void close() {
        mem.release();
    }

    @Override
    public String toString() {
        return "MessageMemTable{" +
                "tabletId=" + tabletId +
                ", beginOffset=" + beginOffset +
                ", endOffset=" + endOffset +
                '}';
    }

    enum AddResultStatus {
        SUCCESS,
        OVERFLOW
    }

    enum GetResultStatus {
        SUCCESS,
        SUBJECT_NOT_FOUND,
        SEQUENCE_TOO_SMALL,
        SEQUENCE_TOO_LARGE,
        TABLE_ALREADY_EVICTED
    }

    public static final class MessageIndex implements MessageFilter.WithTimestamp {
        private final long timestamp;
        private final int position;
        private final int size;

        private MessageIndex(final long timestamp, final int position, final int size) {
            this.timestamp = timestamp;
            this.position = position;
            this.size = size;
        }

        @Override
        public long getTimestamp() {
            return timestamp;
        }

        public int getPosition() {
            return position;
        }

        public int getSize() {
            return size;
        }
    }

    public static final class Entry {
        private final String subject;
        private final long sequence;
        private final long timestamp;
        private final MemTableBuffer data;

        public Entry(final String subject, final long sequence, final long timestamp, final MemTableBuffer data) {
            this.subject = subject;
            this.sequence = sequence;
            this.timestamp = timestamp;
            this.data = data;
        }

        public String getSubject() {
            return subject;
        }

        public long getSequence() {
            return sequence;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public MemTableBuffer getData() {
            return data;
        }
    }

    private static final class TableIterator implements Iterator<Entry> {
        private final MessageMemTable table;
        private final Iterator<String> subjectIter;

        private String currentSubject;
        private long sequence;
        private Iterator<MessageIndex> indexIter;

        private TableIterator(final MessageMemTable table) {
            this.table = table;
            this.table.mem.retain();
            this.subjectIter = table.indexes.keySet().iterator();
        }

        @Override
        public boolean hasNext() {
            if (currentSubject != null && indexIter != null && indexIter.hasNext()) {
                return true;
            }
            if (!subjectIter.hasNext()) {
                table.mem.release();
                return false;
            }
            currentSubject = subjectIter.next();
            sequence = table.firstSequences.get(currentSubject);
            indexIter = table.indexes.get(currentSubject).iterator();
            final boolean hasNext = indexIter.hasNext();
            if (!hasNext) {
                table.mem.release();
            }
            return hasNext;
        }

        @Override
        public Entry next() {
            final MessageIndex index = indexIter.next();
            final MemTableBuffer data = table.selectRecordBuffer(index);
            data.retain();
            final Entry entry = new Entry(currentSubject, sequence, index.getTimestamp(), data);
            sequence += 1;
            return entry;
        }
    }
}