package qunar.tc.qmq.consumer.pull;

import qunar.tc.qmq.ConsumerAllocation;
import qunar.tc.qmq.OrderedPullClient;
import qunar.tc.qmq.PartitionAllocation;

import java.util.List;
import java.util.Set;

/**
 * @author zhenwei.liu
 * @since 2019-09-03
 */
public class OrderedPullConsumer extends CompositePullConsumer<PartitionPullConsumer> implements OrderedPullClient<PartitionPullConsumer> {

    private ConsumerAllocation consumerAllocation;

    public OrderedPullConsumer(List<PartitionPullConsumer> consumers, ConsumerAllocation consumerAllocation) {
        super(consumers);
        this.consumerAllocation = consumerAllocation;
    }

    @Override
    public ConsumerAllocation getConsumerAllocation() {
        return consumerAllocation;
    }
}
