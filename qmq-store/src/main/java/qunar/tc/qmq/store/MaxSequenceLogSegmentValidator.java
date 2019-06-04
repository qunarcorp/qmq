package qunar.tc.qmq.store;

/**
 * @author keli.wang
 * @since 2019-01-18
 */
class MaxSequenceLogSegmentValidator implements LogSegmentValidator {
    private final long maxOffset;

    MaxSequenceLogSegmentValidator(final long maxSequence, final int unitBytes) {
        this((maxSequence + 1) * unitBytes);
    }

    MaxSequenceLogSegmentValidator(final long maxOffset) {
        this.maxOffset = maxOffset;
    }

    @Override
    public ValidateResult validate(final LogSegment segment) {
        final long baseOffset = segment.getBaseOffset();
        final int fileSize = segment.getFileSize();

        if (baseOffset > maxOffset) {
            throw new RuntimeException(String.format("base offset larger than checkpoint max offset. base: %d, max: %d, segment: %s", baseOffset, maxOffset, segment.toString()));
        }

        if (baseOffset + fileSize <= maxOffset) {
            return new ValidateResult(ValidateStatus.COMPLETE, fileSize);
        } else {
            return new ValidateResult(ValidateStatus.PARTIAL, (int) (maxOffset - baseOffset));
        }
    }
}
