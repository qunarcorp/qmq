package qunar.tc.qmq.protocol.consumer;

import qunar.tc.qmq.base.OnOfflineState;
import qunar.tc.qmq.meta.BrokerCluster;
import qunar.tc.qmq.meta.ConsumerAllocation;
import qunar.tc.qmq.protocol.MetaInfoResponse;

/**
 * @author zhenwei.liu
 * @since 2019-08-27
 */
public class ConsumerMetaInfoResponse extends MetaInfoResponse {

    private ConsumerAllocation consumerAllocation;

    public ConsumerMetaInfoResponse(long timestamp, String subject, String consumerGroup, OnOfflineState onOfflineState, int clientTypeCode, BrokerCluster brokerCluster, ConsumerAllocation consumerAllocation) {
        super(timestamp, subject, consumerGroup, onOfflineState, clientTypeCode, brokerCluster);
        this.consumerAllocation = consumerAllocation;
    }

    public ConsumerAllocation getConsumerAllocation() {
        return consumerAllocation;
    }
}
