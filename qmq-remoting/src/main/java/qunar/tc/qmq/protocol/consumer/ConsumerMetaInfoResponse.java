package qunar.tc.qmq.protocol.consumer;

import qunar.tc.qmq.ConsumerAllocation;
import qunar.tc.qmq.protocol.MetaInfoResponse;

/**
 * @author zhenwei.liu
 * @since 2019-08-27
 */
public class ConsumerMetaInfoResponse extends MetaInfoResponse {

    private ConsumerAllocation consumerAllocation;

    public ConsumerAllocation getConsumerAllocation() {
        return consumerAllocation;
    }

    public ConsumerMetaInfoResponse setConsumerAllocation(ConsumerAllocation consumerAllocation) {
        this.consumerAllocation = consumerAllocation;
        return this;
    }
}
