package qunar.tc.qmq.metainfoclient;

import qunar.tc.qmq.base.OnOfflineState;

/**
 * @author zhenwei.liu
 * @since 2019-08-29
 */
public interface MetaInfoService {

    void reportConsumerState(String subject, String consumerGroup, String clientId, OnOfflineState state);
}
