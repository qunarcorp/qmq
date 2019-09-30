package qunar.tc.qmq.protocol.consumer;

/**
 * @author zhenwei.liu
 * @since 2019-09-18
 */
public class ReleasePullLockRequest {

    private String partitionName;
    private String consumerGroup;
    private String clientId;

    public ReleasePullLockRequest(String partitionName, String consumerGroup, String clientId) {
        this.partitionName = partitionName;
        this.consumerGroup = consumerGroup;
        this.clientId = clientId;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public String getClientId() {
        return clientId;
    }
}
