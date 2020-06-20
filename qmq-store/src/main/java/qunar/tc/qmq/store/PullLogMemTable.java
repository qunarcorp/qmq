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

import io.netty.buffer.ByteBuf;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by zhaohui.yu
 * 2020/6/6
 */
public class PullLogMemTable extends MemTable {

    public static final int SEQUENCE_SIZE = 10 * 1024 * 1024;

    private final ConcurrentMap<String, PullLogSequence> messageSequences = new ConcurrentHashMap<>();

    public static final int ENTRY_SIZE = Integer.BYTES;

    private volatile int writerIndex;

    public PullLogMemTable(final long tabletId, final long beginOffset, final int capacity) {
        super(tabletId, beginOffset, capacity);
    }

    @Override
    public boolean checkWritable(final int writeBytes) {
        return getCapacity() - writerIndex > writeBytes;
    }

    public void putPullLogMessages(String subject, String group, String consumerId, List<PullLogMessage> messages) {
        PullLogSequence pullLogSequence = messageSequences.computeIfAbsent(keyOf(subject, group, consumerId), k -> new PullLogSequence());
        for (PullLogMessage message : messages) {
            pullLogSequence.add(message.getSequence(), message.getMessageSequence());
        }
        writerIndex += (messages.size() * ENTRY_SIZE);
    }

    public void ack(String subject, String group, String consumerId, long firstSequence, long lastSequence) {
        PullLogSequence pullLogSequence = messageSequences.get(keyOf(subject, group, consumerId));
        if (pullLogSequence == null) return;

        //ack的范围已经被挤出了内存
        if (lastSequence < pullLogSequence.basePullSequence) return;
        //这种情况一般不会出现，ack的返回还没回放
        if (firstSequence > (pullLogSequence.basePullSequence + pullLogSequence.messageOffsets.size() - 1)) return;
        //这也是异常情况，ack不连续，ack了中间的一个区间，应该是什么地方出问题了
        if (firstSequence > pullLogSequence.basePullSequence) return;

        //ack的范围覆盖了多少pull log
        int ackSize = (int) (lastSequence - pullLogSequence.basePullSequence);
        pullLogSequence.basePullSequence = lastSequence;
        //将已经ack的pull log截断
        pullLogSequence.messageOffsets.cut(ackSize + 1);
        writerIndex -= ackSize * ENTRY_SIZE;
    }

    public static String keyOf(String subject, String group, String consumerId) {
        return consumerId + "@" + group + "@" + subject;
    }

    public void dump(ByteBuf buffer, Map<String, PullLogIndexEntry> indexMap) {
        for (Map.Entry<String, PullLogSequence> entry : messageSequences.entrySet()) {
            PullLogIndexEntry indexEntry = new PullLogIndexEntry(entry.getValue().basePullSequence, entry.getValue().baseMessageSequence, buffer.writerIndex());
            indexMap.put(entry.getKey(), indexEntry);
            IntArrayList messageOffsets = entry.getValue().messageOffsets;
            for (int i = 0; i < messageOffsets.size(); ++i) {
                buffer.writeInt(messageOffsets.get(i));
            }
        }
    }

    public long getConsumerLogSequence(String subject, String group, String consumerId, long pullSequence) {
        String key = keyOf(subject, group, consumerId);
        PullLogSequence pullLogSequence = messageSequences.get(key);
        if (pullLogSequence == null) return -1L;

        int offset = (int) (pullSequence - pullLogSequence.basePullSequence);
        if (offset < 0 || offset >= pullLogSequence.messageOffsets.size()) return -1L;
        int messageSequenceOffset = pullLogSequence.messageOffsets.get(offset);
        return pullLogSequence.baseMessageSequence + messageSequenceOffset;
    }

    @Override
    public void close() {

    }

    @Override
    public int getTotalDataSize() {
        return 0;
    }

    private static class PullLogSequence {

        private long basePullSequence = -1L;

        private long baseMessageSequence = -1L;

        private final IntArrayList messageOffsets = new IntArrayList();

        public void add(long pullSequence, long messageSequence) {
            if (basePullSequence == -1L) {
                basePullSequence = pullSequence;
            }
            if (baseMessageSequence == -1L) {
                baseMessageSequence = messageSequence;
            }

            int offset = (int) (messageSequence - baseMessageSequence);
            messageOffsets.add(offset);
        }
    }
}
