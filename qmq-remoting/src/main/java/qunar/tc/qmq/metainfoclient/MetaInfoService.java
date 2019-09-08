package qunar.tc.qmq.metainfoclient;

import com.google.common.util.concurrent.ListenableFuture;
import qunar.tc.qmq.base.ClientRequestType;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.protocol.MetaInfoResponse;

/**
 * @author zhenwei.liu
 * @since 2019-08-29
 */
public interface MetaInfoService {

    void triggerConsumerMetaInfoRequest(ClientRequestType requestType);

    void registerHeartbeat(String subject, String group, ClientType clientType, String appCode);

    ListenableFuture<MetaInfoResponse> request(DefaultMetaInfoService.MetaInfoRequestParam param, ClientRequestType requestType);

    ListenableFuture<MetaInfoResponse> request(String subject, String group, ClientType clientType, String appCode, ClientRequestType requestType);

    void registerResponseSubscriber(MetaInfoClient.ResponseSubscriber subscriber);
}
