package qunar.tc.qmq.store;

import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.nio.ByteBuffer;

/**
 * @author keli.wang
 * @since 2017/8/21
 */
public class IndexLogVisitor extends AbstractLogVisitor<MessageQueryIndex> {

    IndexLogVisitor(final LogManager logManager, final long startOffset) {
        super(logManager, startOffset);
    }

    @Override
    protected LogVisitorRecord<MessageQueryIndex> readOneRecord(SegmentBuffer segmentBuffer) {
        final ByteBuffer buffer = segmentBuffer.getBuffer();
        final int startPos = buffer.position();
        // magic
        if (buffer.remaining() < Long.BYTES) {
            //end of file
            return retNoMore();
        }

        // sequence
        long sequence = buffer.getLong();
        if (sequence < 0) {
            return retNoMore();
        }

        // createTime
        if (buffer.remaining() < Long.BYTES) {
            return retNoMore();
        }
        long createTime = buffer.getLong();

        // subject
        if (buffer.remaining() < Short.BYTES) {
            return retNoMore();
        }
        short subjectLen = buffer.getShort();
        if (buffer.remaining() < subjectLen) {
            return retNoMore();
        }
        String subject = PayloadHolderUtils.readString(subjectLen, buffer);

        // msgId
        if (buffer.remaining() < Short.BYTES) {
            return retNoMore();
        }
        short messageIdLen = buffer.getShort();
        if (buffer.remaining() < messageIdLen) {
            return retNoMore();
        }
        String messageId = PayloadHolderUtils.readString(messageIdLen, buffer);

        MessageQueryIndex index = new MessageQueryIndex(subject, messageId, sequence, createTime);
        incrVisitedBufferSize(buffer.position() - startPos);
        return LogVisitorRecord.data(index);
    }

    private LogVisitorRecord<MessageQueryIndex> retNoMore() {
        setVisitedBufferSize(getBufferSize());
        return LogVisitorRecord.noMore();
    }
}
