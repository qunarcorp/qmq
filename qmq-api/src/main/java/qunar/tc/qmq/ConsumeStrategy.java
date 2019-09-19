package qunar.tc.qmq;

/**
 * @author zhenwei.liu
 * @since 2019-09-16
 */
public enum ConsumeStrategy {

    EXCLUSIVE, SHARED;

    public static ConsumeStrategy getConsumerStrategy(boolean isBroadcast, boolean isOrdered) {
        return isBroadcast || isOrdered ? ConsumeStrategy.EXCLUSIVE : ConsumeStrategy.SHARED;
    }
}
