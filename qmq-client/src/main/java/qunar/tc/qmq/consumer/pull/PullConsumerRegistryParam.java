package qunar.tc.qmq.consumer.pull;

import qunar.tc.qmq.StatusSource;

/**
 * @author zhenwei.liu
 * @since 2019-09-27
 */
public class PullConsumerRegistryParam {

    private boolean isBroadcast;
    private boolean isOrdered;
    private StatusSource statusSource;

    public PullConsumerRegistryParam(boolean isBroadcast, boolean isOrdered, StatusSource statusSource) {
        this.isBroadcast = isBroadcast;
        this.isOrdered = isOrdered;
        this.statusSource = statusSource;
    }

    public boolean isBroadcast() {
        return isBroadcast;
    }

    public boolean isOrdered() {
        return isOrdered;
    }

    public StatusSource getStatusSource() {
        return statusSource;
    }
}
