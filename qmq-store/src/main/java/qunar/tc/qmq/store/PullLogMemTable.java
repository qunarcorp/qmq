package qunar.tc.qmq.store;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
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

    private int writerIndex;

    public PullLogMemTable(final long tabletId, final long beginOffset, final int capacity) {
        super(tabletId, beginOffset, capacity);
    }

    @Override
    public int getTotalDataSize() {
        return 0;
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

    public static String keyOf(String subject, String group, String consumerId) {
        return consumerId + "@" + group + "@" + subject;
    }

    public void dump(ByteBuf buffer, Map<String, PullLogIndexEntry> indexMap) {
        for (Map.Entry<String, PullLogSequence> entry : messageSequences.entrySet()) {
            PullLogIndexEntry indexEntry = new PullLogIndexEntry(entry.getValue().basePullSequence, entry.getValue().baseMessageSequence, buffer.writerIndex());
            indexMap.put(entry.getKey(), indexEntry);
            for (Integer offset : entry.getValue().messageOffsets) {
                buffer.writeInt(offset);
            }
        }
    }

    public long getConsumerLogSequence(String subject, String group, String consumerId, long pullSequence) {
        String key = keyOf(subject, group, consumerId);
        PullLogSequence pullLogSequence = messageSequences.get(key);
        if (pullLogSequence == null) return -1L;

        int offset = (int) (pullSequence - pullLogSequence.basePullSequence);
        if (offset < 0 || offset >= pullLogSequence.messageOffsets.size()) return -1L;
        Integer messageSequenceOffset = pullLogSequence.messageOffsets.get(offset);
        return pullLogSequence.baseMessageSequence + messageSequenceOffset;
    }

    @Override
    public void close() {

    }

    private static class PullLogSequence {

        private long basePullSequence = -1L;

        private long baseMessageSequence = -1L;

        private List<Integer> messageOffsets = new ArrayList<>();

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
