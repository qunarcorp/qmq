package qunar.tc.qmq.store;

import qunar.tc.qmq.base.PullMessageResult;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.protocol.consumer.PullFilter;
import qunar.tc.qmq.protocol.consumer.PullRequest;
import qunar.tc.qmq.store.buffer.Buffer;

import java.util.ArrayList;
import java.util.List;

public abstract class MessageReader {

    private final PullMessageFilterChain pullMessageFilterChain;

    public MessageReader(DynamicConfig config) {
        this.pullMessageFilterChain = new PullMessageFilterChain(config);
    }

    public abstract PullMessageResult findMessages(final PullRequest pullRequest);

    protected void releaseRemain(int startIndex, List<GetMessageResult> list) {
        for (int i = startIndex; i < list.size(); ++i) {
            list.get(i).release();
        }
    }

    protected boolean noPullFilter(PullRequest pullRequest) {
        final List<PullFilter> filters = pullRequest.getFilters();
        return filters == null || filters.isEmpty();
    }

    protected boolean needKeep(PullRequest request, Buffer message) {
        return pullMessageFilterChain.needKeep(request, message);
    }

    /**
     * 过滤掉不需要的消息，这样拿出来的一块连续的消息就不连续了，比如1 - 100条消息
     * 中间20,21,50,73这4条不符合条件，则返回的是
     * [1, 19], [22, 49], [51, 72], [74, 100] 这几个连续的段
     */
    protected List<GetMessageResult> filter(PullRequest request, GetMessageResult input) {
        List<GetMessageResult> result = new ArrayList<>();

        List<Buffer> messages = input.getBuffers();
        OffsetRange offsetRange = input.getConsumerLogRange();

        GetMessageResult range = null;
        long begin = -1;
        long end = -1;
        for (int i = 0; i < messages.size(); ++i) {
            Buffer message = messages.get(i);
            if (needKeep(request, message)) {
                if (range == null) {
                    range = new GetMessageResult();
                    result.add(range);
                    begin = offsetRange.getBegin() + i;
                }
                end = offsetRange.getBegin() + i;
                range.addBuffer(message);
            } else {
                message.release();
                setOffsetRange(range, begin, end);
                range = null;
            }
        }
        setOffsetRange(range, begin, end);
        appendEmpty(end, offsetRange, result);
        return result;
    }

    private void setOffsetRange(GetMessageResult input, long begin, long end) {
        if (input != null) {
            input.setConsumerLogRange(new OffsetRange(begin, end));
            input.setNextBeginSequence(end + 1);
        }
    }

    /*
    begin=0                           end=8
    ------------------------------------
    | - | - | - | + | + | + | + | + | + |
    -------------------------------------
    shift -> begin=3, end=8
     */
    protected void shiftRight(GetMessageResult getMessageResult) {
        OffsetRange offsetRange = getMessageResult.getConsumerLogRange();
        long expectedBegin = offsetRange.getEnd() - getMessageResult.getMessageNum() + 1;
        if (expectedBegin == offsetRange.getBegin()) return;
        getMessageResult.setConsumerLogRange(new OffsetRange(expectedBegin, offsetRange.getEnd()));
    }

    private void appendEmpty(long end, OffsetRange offsetRange, List<GetMessageResult> list) {
        if (end < offsetRange.getEnd()) {
            GetMessageResult emptyRange = new GetMessageResult();
            long begin = end == -1 ? offsetRange.getBegin() : end;
            emptyRange.setConsumerLogRange(new OffsetRange(begin, offsetRange.getEnd()));
            emptyRange.setNextBeginSequence(offsetRange.getEnd() + 1);
            list.add(emptyRange);
        }
    }
}
