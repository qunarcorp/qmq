package qunar.tc.qmq.meta.event;

import com.google.common.eventbus.Subscribe;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.base.ClientRequestType;
import qunar.tc.qmq.meta.model.ClientMetaInfo;
import qunar.tc.qmq.meta.order.PartitionAllocationTask;
import qunar.tc.qmq.meta.store.ClientMetaInfoStore;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;

import java.util.Objects;

/**
 * @author zhenwei.liu
 * @since 2019-08-28
 */
public class OrderedConsumerHeartbeatHandler implements HeartbeatHandler {

    private ClientMetaInfoStore clientMetaInfoStore;
    private PartitionAllocationTask partitionAllocationTask;

    public OrderedConsumerHeartbeatHandler(ClientMetaInfoStore clientMetaInfoStore, PartitionAllocationTask partitionAllocationTask) {
        this.clientMetaInfoStore = clientMetaInfoStore;
        this.partitionAllocationTask = partitionAllocationTask;
    }

    @Override
    @Subscribe
    public void handle(MetaInfoRequest request) {
        String subject = request.getSubject();
        String consumerGroup = request.getConsumerGroup();
        int requestType = request.getRequestType();

        int clientTypeCode = request.getClientTypeCode();
        ClientType clientType = ClientType.of(clientTypeCode);

        ConsumeStrategy consumerStrategy = ConsumeStrategy.getConsumerStrategy(request.isBroadcast(), request.isOrdered());
        if (Objects.equals(consumerStrategy, ConsumeStrategy.EXCLUSIVE) && clientType.isConsumer()) {
            // 更新在线状态
            ClientMetaInfo clientMetaInfo = new ClientMetaInfo();
            clientMetaInfo.setSubject(subject);
            clientMetaInfo.setConsumerGroup(consumerGroup);
            clientMetaInfo.setClientTypeCode(clientTypeCode);
            clientMetaInfo.setClientId(request.getClientId());
            clientMetaInfo.setOnlineStatus(request.getOnlineState());

            clientMetaInfoStore.updateClientOnlineState(clientMetaInfo);

            if (ClientRequestType.SWITCH_STATE.getCode() == requestType) {
                // 触发重分配
                this.partitionAllocationTask.reallocation(subject, consumerGroup);
            }
        }
    }
}
