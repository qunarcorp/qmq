package qunar.tc.qmq.metainfoclient;

import com.google.common.util.concurrent.ListenableFuture;
import qunar.tc.qmq.ConsumeMode;
import qunar.tc.qmq.base.ClientRequestType;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.protocol.MetaInfoResponse;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;

/**
 * @author zhenwei.liu
 * @since 2019-08-29
 */
public interface MetaInfoService {

    void triggerConsumerMetaInfoRequest(ClientRequestType requestType);

    void registerHeartbeat(String subject, String consumerGroup, ClientType clientType, String appCode);

    void registerHeartbeat(String subject, String consumerGroup, ClientType clientType, String appCode, ConsumeMode consumeMode);

    ListenableFuture<MetaInfoResponse> request(MetaInfoRequest request);

    void registerResponseSubscriber(MetaInfoClient.ResponseSubscriber subscriber);
}
