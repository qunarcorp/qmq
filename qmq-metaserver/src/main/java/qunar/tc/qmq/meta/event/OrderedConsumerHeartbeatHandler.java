package qunar.tc.qmq.meta.event;

import qunar.tc.qmq.base.ClientRequestType;
import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.meta.PartitionMapping;
import qunar.tc.qmq.meta.cache.CachedMetaInfoManager;
import qunar.tc.qmq.meta.model.ClientMetaInfo;
import qunar.tc.qmq.meta.store.ClientMetaInfoStore;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;

/**
 * @author zhenwei.liu
 * @since 2019-08-28
 */
public class OrderedConsumerHeartbeatHandler implements HeartbeatHandler {

    private CachedMetaInfoManager cachedMetaInfoManager;
    private ClientMetaInfoStore clientMetaInfoStore;

    public OrderedConsumerHeartbeatHandler(CachedMetaInfoManager cachedMetaInfoManager, ClientMetaInfoStore clientMetaInfoStore) {
        this.cachedMetaInfoManager = cachedMetaInfoManager;
        this.clientMetaInfoStore = clientMetaInfoStore;
    }

    @Override
    public void handle(MetaInfoRequest request) {
        String subject = request.getSubject();
        int requestType = request.getRequestType();

        PartitionMapping partitionMapping = cachedMetaInfoManager.getPartitionMapping(subject);
        int clientTypeCode = request.getClientTypeCode();
        ClientType clientType = ClientType.of(clientTypeCode);

        if (partitionMapping != null &&
                ClientRequestType.HEARTBEAT.getCode() == requestType
                && clientType.isConsumer()) {
            // ordered message consumer heartbeat
            ClientMetaInfo clientMetaInfo = new ClientMetaInfo();
            clientMetaInfo.setSubject(request.getSubject());
            clientMetaInfo.setConsumerGroup(request.getConsumerGroup());
            clientMetaInfo.setClientTypeCode(clientTypeCode);
            clientMetaInfo.setClientId(request.getClientId());
            clientMetaInfo.setOnlineStatus(ClientMetaInfo.OnlineStatus.ONLINE);

            clientMetaInfoStore.updateClientOnlineState(clientMetaInfo);
        }
    }
}
