package qunar.tc.qmq.producer.sender;

import qunar.tc.qmq.Message;
import qunar.tc.qmq.MessageGroup;

/**
 * 消息分组, 最终会将消息对应到一个 brokerGroup 的一个 subject 上
 *
 * @author zhenwei.liu
 * @since 2019-09-06
 */
public interface MessageGroupResolver {

    /**
     * 选择一个 brokerGroup, 该 brokerGroup 不一定可用
     * @param message 消息
     * @return brokerGroup
     */
    MessageGroup resolveGroup(Message message);

    /**
     * 选择一个可用的 brokerGroup, 如不可用则选择下一个可用的 brokerGroup, 但必须保证 PartitionName 在这两个 BrokerGroup 是相同的
     * @param message message
     * @return 可用的 brokerGroup
     */
    MessageGroup resolveAvailableGroup(Message message);
}
