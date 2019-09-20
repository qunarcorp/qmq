package qunar.tc.qmq.metainfoclient;

import com.google.common.util.concurrent.ListenableFuture;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.base.ClientRequestType;
import qunar.tc.qmq.meta.Partition;
import qunar.tc.qmq.meta.ProducerAllocation;
import qunar.tc.qmq.protocol.MetaInfoResponse;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-08-29
 */
public interface MetaInfoService {

    /**
     * 通过 cache 中的 request 来触发心跳
     */
    ListenableFuture<MetaInfoResponse> triggerHeartbeat(int clientTypeCode, String subject, String consumerGroup);

    /**
     * 注册心跳, 后续会定时发送
     */
    ListenableFuture<MetaInfoResponse> registerHeartbeat(String appCode, int clientTypeCode, String subject, String consumerGroup, boolean isBroadcast, boolean isOrdered);

    /**
     * 发送请求, 且不缓存, 这是为了解决缓存
     */
    ListenableFuture<MetaInfoResponse> sendRequest(MetaInfoRequest request);

    String getSubject(String partitionName);

    void registerResponseSubscriber(MetaInfoClient.ResponseSubscriber subscriber);
}
