package qunar.tc.qmq.config;

import com.google.common.collect.Maps;
import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.meta.PartitionAllocation;
import qunar.tc.qmq.meta.PartitionMapping;
import qunar.tc.qmq.metainfoclient.MetaInfoClient;
import qunar.tc.qmq.metainfoclient.MetaInfoService;
import qunar.tc.qmq.protocol.MetaInfoResponse;
import qunar.tc.qmq.protocol.producer.ProducerMetaInfoResponse;

import java.util.Map;

/**
 * @author zhenwei.liu
 * @since 2019-08-21
 */
public class DefaultOrderedMessageManager implements OrderedMessageManager, MetaInfoClient.ResponseSubscriber {

    private Map<String, PartitionMapping> partitionMap = Maps.newConcurrentMap();

    public DefaultOrderedMessageManager(MetaInfoService metaInfoService) {
        metaInfoService.registerResponseSubscriber(this);
    }

    @Override
    public PartitionMapping getPartitionMapping(String subject) {
        return partitionMap.get(subject);
    }

    @Override
    public void onResponse(MetaInfoResponse response) {
        ClientType clientType = ClientType.of(response.getClientTypeCode());
        if (clientType.isProducer()) {
            ProducerMetaInfoResponse producerResponse = (ProducerMetaInfoResponse) response;
            String subject = response.getSubject();
            PartitionMapping partitionMapping = producerResponse.getPartitionMapping();
            if (partitionMapping == null) return;
            PartitionMapping old = partitionMap.putIfAbsent(subject, partitionMapping);
            if (old == null) {
                // 旧的存在, 对比版本
                partitionMap.computeIfPresent(subject, (key, oldMapping) -> {
                    int oldVersion = oldMapping.getVersion();
                    int newVersion = partitionMapping.getVersion();
                    return newVersion > oldVersion ? partitionMapping : oldMapping;
                });
            }
        } else if (clientType.isConsumer()) {
            // TODO(zhenwei.liu)
        }
    }
}
