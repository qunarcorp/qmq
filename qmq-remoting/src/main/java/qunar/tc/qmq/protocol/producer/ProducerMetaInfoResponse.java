package qunar.tc.qmq.protocol.producer;

import qunar.tc.qmq.base.OnOfflineState;
import qunar.tc.qmq.meta.BrokerCluster;
import qunar.tc.qmq.meta.ProducerAllocation;
import qunar.tc.qmq.protocol.MetaInfoResponse;

/**
 * @author zhenwei.liu
 * @since 2019-08-27
 */
public class ProducerMetaInfoResponse extends MetaInfoResponse {

    private ProducerAllocation producerAllocation;

    public ProducerMetaInfoResponse(long timestamp, String subject, String consumerGroup, OnOfflineState onOfflineState, int clientTypeCode, BrokerCluster brokerCluster, ProducerAllocation producerAllocation) {
        super(timestamp, subject, consumerGroup, onOfflineState, clientTypeCode, brokerCluster);
        this.producerAllocation = producerAllocation;
    }

    public ProducerAllocation getProducerAllocation() {
        return producerAllocation;
    }
}
