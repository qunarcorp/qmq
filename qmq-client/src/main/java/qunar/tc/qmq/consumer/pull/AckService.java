package qunar.tc.qmq.consumer.pull;

import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.broker.BrokerGroupInfo;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-09-18
 */
public interface AckService {

    interface SendAckCallback {

        void success();

        void fail(Exception ex);
    }

    List<PulledMessage> buildPulledMessages(PullParam pullParam, PullResult pullResult, AckSendQueue sendQueue, AckHook ackHook, PulledMessageFilter filter);

    void sendAck(BrokerGroupInfo brokerGroup, String subject, String consumerGroup, ConsumeStrategy consumeStrategy, AckSendEntry ack, SendAckCallback callback);

    void setClientId(String clientId);
}
