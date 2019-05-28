package qunar.tc.qmq.store;

import java.nio.ByteBuffer;

/**
 * @author keli.wang
 * @since 2017/8/23
 */
public class MessageLogRecord {
    private final String subject;
    private final long sequence;
    private final long wroteOffset;
    private final int wroteBytes;
    private final long payloadOffset;
    private final long baseOffset;
    private final ByteBuffer payload;
    private final LogSegment logSegment;

    public MessageLogRecord(String subject,
                            long sequence,
                            long wroteOffset,
                            int wroteBytes,
                            long payloadOffset,
                            long baseOffset,
                            ByteBuffer payload,
                            LogSegment logSegment) {
        this.subject = subject;
        this.sequence = sequence;
        this.wroteOffset = wroteOffset;
        this.wroteBytes = wroteBytes;
        this.payloadOffset = payloadOffset;
        this.baseOffset = baseOffset;
        this.payload = payload;
        this.logSegment = logSegment;
    }

    public String getSubject() {
        return subject;
    }

    public long getSequence() {
        return sequence;
    }

    public long getWroteOffset() {
        return wroteOffset;
    }

    public int getWroteBytes() {
        return wroteBytes;
    }

    public long getPayloadOffset() {
        return payloadOffset;
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    public ByteBuffer getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "MessageLogRecord{" +
                "subject='" + subject + '\'' +
                ", sequence=" + sequence +
                ", wroteOffset=" + wroteOffset +
                ", wroteBytes=" + wroteBytes +
                ", payloadOffset=" + payloadOffset +
                ", baseOffset=" + baseOffset +
                '}';
    }

    public LogSegment getLogSegment() {
        return logSegment;
    }
}
