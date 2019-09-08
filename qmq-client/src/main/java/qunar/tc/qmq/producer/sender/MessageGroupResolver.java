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

    MessageGroup resolveGroup(Message message);
}
