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

    ListenableFuture<MetaInfoResponse> request(int clientTypeCode, String subject, String consumerGroup);

    ListenableFuture<MetaInfoResponse> request(MetaInfoRequest request);

    String getSubject(String partitionName);

    void registerResponseSubscriber(MetaInfoClient.ResponseSubscriber subscriber);
}
