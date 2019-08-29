package qunar.tc.qmq.metainfoclient;

/**
 * @author zhenwei.liu
 * @since 2019-08-29
 */
public interface ConsumerOnlineStateManager {

    interface OnlineStateGetter {

        boolean isOnline();
    }

    boolean isOnline(String subject, String group, String clientId);

    void registerConsumer(String subject, String group, String clientId, OnlineStateGetter onlineStateGetter);
}
