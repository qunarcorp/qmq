package qunar.tc.qmq.delay.sender.loadbalance;

import qunar.tc.qmq.broker.BrokerGroupInfo;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019-01-08 15:08
 */
public class BrokerGroupStats {
    private final BrokerGroupInfo brokerGroupInfo;

    // send time

    // send failed

    // send success

    // to send count
    private final AtomicLong toSend;

    public BrokerGroupStats(final BrokerGroupInfo brokerGroupInfo) {
        this.brokerGroupInfo = brokerGroupInfo;
        this.toSend = new AtomicLong(0);
    }

    public void incrementToSendCount(long toSendNum) {
        toSend.addAndGet(toSendNum);
    }

    public void decrementToSendCount(long sendNum) {
        long count = toSend.get();
        toSend.set(count - sendNum);
    }

    long getToSendCount() {
        return toSend.get();
    }
}
