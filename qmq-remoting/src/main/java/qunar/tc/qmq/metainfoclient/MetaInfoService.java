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
    void triggerHeartbeat(int clientTypeCode, String subject, String consumerGroup);

    /**
     * 注册心跳, 后续会定时发送
     */
    void registerHeartbeat(String appCode, int clientTypeCode, String subject, String consumerGroup, boolean isBroadcast, boolean isOrdered, boolean healthCheckOnlineState);

    void sendRequest(MetaInfoRequest request);

    String getSubject(String partitionName);

    void registerResponseSubscriber(MetaInfoClient.ResponseSubscriber subscriber);
}
