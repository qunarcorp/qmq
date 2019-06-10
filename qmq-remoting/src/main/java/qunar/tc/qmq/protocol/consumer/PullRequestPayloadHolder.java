package qunar.tc.qmq.protocol.consumer;

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.protocol.PayloadHolder;

/**
 * @author yiqun.fan create on 17-8-2.
 */
public class PullRequestPayloadHolder implements PayloadHolder {
    private static final PullRequestSerde SERDE = new PullRequestSerde();

    private final PullRequest request;

    public PullRequestPayloadHolder(PullRequest request) {
        this.request = request;
    }

    @Override
    public void writeBody(ByteBuf out) {
        SERDE.write(request, out);
    }
}
