package qunar.tc.qmq.config;

import com.google.common.collect.Maps;
import qunar.tc.qmq.meta.PartitionInfo;
import qunar.tc.qmq.metainfoclient.MetaInfoClient;
import qunar.tc.qmq.metainfoclient.MetaInfoService;
import qunar.tc.qmq.protocol.consumer.MetaInfoResponse;

import java.util.Map;

/**
 * @author zhenwei.liu
 * @since 2019-08-21
 */
public class DefaultOrderedMessageManager implements OrderedMessageManager, MetaInfoClient.ResponseSubscriber {

    private Map<String, PartitionInfo> partitionMap = Maps.newConcurrentMap();

    public DefaultOrderedMessageManager(MetaInfoService metaInfoService) {
        metaInfoService.registerResponseSubscriber(this);
    }

    @Override
    public PartitionInfo getPartitionInfo(String subject) {
        return partitionMap.get(subject);
    }

    @Override
    public void onResponse(MetaInfoResponse response) {
        String subject = response.getSubject();
        PartitionInfo partitionInfo = response.getPartitionInfo();
        PartitionInfo old = partitionMap.putIfAbsent(subject, partitionInfo);
        if (old == null) {
            // 旧的存在, 对比版本
            partitionMap.computeIfPresent(subject, (key, value) -> {
                int oldVersion = value.getVersion();
                int newVersion = partitionInfo.getVersion();
                return newVersion > oldVersion ? partitionInfo : value;
            });
        }
    }
}
