package qunar.tc.qmq.common;

import qunar.tc.qmq.Message;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.utils.DelayUtil;

/**
 * @author zhenwei.liu
 * @since 2019-08-19
 */
public class OrderedMessageUtils {

    public static boolean isOrderedMessage(Message message) {
        // 延迟消息不走顺序逻辑
        return message.getOrderKey() != null && !DelayUtil.isDelayMessage(message);
    }

    public static String getOrderedMessageSubject(String subject, int physicalPartition) {
        return subject + "#" + physicalPartition;
    }

    public static String getRealSubject(Message message) {
        int partition = message.getIntProperty(BaseMessage.keys.qmq_physicalPartition.name());
        String subject = message.getSubject();
        return subject.replace("#" + partition, "");
    }
}
