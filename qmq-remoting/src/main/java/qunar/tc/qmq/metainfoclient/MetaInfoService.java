package qunar.tc.qmq.metainfoclient;

import qunar.tc.qmq.base.ClientRequestType;

/**
 * @author zhenwei.liu
 * @since 2019-08-29
 */
public interface MetaInfoService {

    void triggerConsumerMetaInfoRequest(boolean isOrdered, ClientRequestType requestType);
}
