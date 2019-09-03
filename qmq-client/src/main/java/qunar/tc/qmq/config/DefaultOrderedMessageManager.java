package qunar.tc.qmq.config;

import com.google.common.collect.Maps;
import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.PartitionAllocation;
import qunar.tc.qmq.meta.PartitionMapping;
import qunar.tc.qmq.metainfoclient.DefaultMetaInfoService;
import qunar.tc.qmq.metainfoclient.MetaInfoClient;
import qunar.tc.qmq.protocol.MetaInfoResponse;
import qunar.tc.qmq.protocol.consumer.ConsumerMetaInfoResponse;
import qunar.tc.qmq.protocol.producer.ProducerMetaInfoResponse;

import java.util.Map;

/**
 * @author zhenwei.liu
 * @since 2019-08-21
 */
public class DefaultOrderedMessageManager implements OrderedMessageManager, MetaInfoClient.ResponseSubscriber {

    private Map<String, PartitionMapping> partitionMap = Maps.newConcurrentMap();
    private Map<String, PartitionAllocation> allocationMap = Maps.newConcurrentMap();

    public DefaultOrderedMessageManager(DefaultMetaInfoService metaInfoService) {
        metaInfoService.registerResponseSubscriber(this);
    }

    @Override
    public PartitionMapping getPartitionMapping(String subject) {
        return partitionMap.get(subject);
    }

    @Override
    public PartitionAllocation getPartitionAllocation(String subject, String group) {
        String key = createPartitionAllocationKey(subject, group);
        return allocationMap.get(key);
    }

    private String createPartitionAllocationKey(String subject, String group) {
        return subject + ":" + group;
    }

    @Override
    public void onResponse(MetaInfoResponse response) {
        ClientType clientType = ClientType.of(response.getClientTypeCode());
        if (clientType.isProducer()) {
            ProducerMetaInfoResponse producerResponse = (ProducerMetaInfoResponse) response;
            PartitionMapping partitionMapping = producerResponse.getPartitionMapping();
            if (partitionMapping == null) return;
            String subject = response.getSubject();
            PartitionMapping old = partitionMap.putIfAbsent(subject, partitionMapping);
            if (old != null) {
                // 旧的存在, 对比版本
                partitionMap.computeIfPresent(subject, (key, oldMapping) -> {
                    int oldVersion = oldMapping.getVersion();
                    int newVersion = partitionMapping.getVersion();
                    return newVersion > oldVersion ? partitionMapping : oldMapping;
                });
            }
        } else if (clientType.isConsumer()) {
            ConsumerMetaInfoResponse consumerResponse = (ConsumerMetaInfoResponse) response;
            PartitionAllocation partitionAllocation = consumerResponse.getPartitionAllocation();
            if (partitionAllocation == null) return;
            String subject = response.getSubject();
            String group = response.getConsumerGroup();
            String key = createPartitionAllocationKey(subject, group);
            PartitionAllocation old = allocationMap.putIfAbsent(key, partitionAllocation);
            if (old != null) {
                allocationMap.computeIfPresent(key, (k, oldAllocation) -> {
                    int oldVersion = oldAllocation.getVersion();
                    int newVersion = partitionAllocation.getVersion();
                    return newVersion > oldVersion ? partitionAllocation : oldAllocation;
                });
            }
        }
    }
}
