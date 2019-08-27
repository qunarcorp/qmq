package qunar.tc.qmq.config;

import com.google.common.collect.Maps;
import qunar.tc.qmq.meta.PartitionAllocation;
import qunar.tc.qmq.metainfoclient.MetaInfoClient;
import qunar.tc.qmq.metainfoclient.MetaInfoService;
import qunar.tc.qmq.protocol.MetaInfoResponse;

import java.util.Map;

/**
 * @author zhenwei.liu
 * @since 2019-08-21
 */
public class DefaultOrderedMessageManager implements OrderedMessageManager, MetaInfoClient.ResponseSubscriber {

    private Map<String, PartitionAllocation> partitionMap = Maps.newConcurrentMap();

    public DefaultOrderedMessageManager(MetaInfoService metaInfoService) {
        metaInfoService.registerResponseSubscriber(this);
    }

    @Override
    public PartitionAllocation getPartitionInfo(String subject) {
        return partitionMap.get(subject);
    }

    @Override
    public void onResponse(MetaInfoResponse response) {
        String subject = response.getSubject();
        PartitionAllocation partitionAllocation = response.getPartitionAllocation();
        if (partitionAllocation == null) return;
        PartitionAllocation old = partitionMap.putIfAbsent(subject, partitionAllocation);
        if (old == null) {
            // 旧的存在, 对比版本
            partitionMap.computeIfPresent(subject, (key, value) -> {
                int oldVersion = value.getVersion();
                int newVersion = partitionAllocation.getVersion();
                return newVersion > oldVersion ? partitionAllocation : value;
            });
        }
    }
}
