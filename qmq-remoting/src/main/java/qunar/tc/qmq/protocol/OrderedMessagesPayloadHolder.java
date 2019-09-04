package qunar.tc.qmq.protocol;

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.common.OrderedMessageUtils;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-09-04
 */
public class OrderedMessagesPayloadHolder extends MessagesPayloadHolder {

    public OrderedMessagesPayloadHolder(List<BaseMessage> messages) {
        super(messages);
    }

    @Override
    protected void writeSubject(BaseMessage message, ByteBuf out) {
        if (OrderedMessageUtils.isOrderedMessage(message)) {
            int partition = message.getIntProperty(BaseMessage.keys.qmq_physicalPartition.name());
            String orderedSubject = OrderedMessageUtils.getOrderedMessageSubject(message.getSubject(), partition);
            PayloadHolderUtils.writeString(orderedSubject, out);
        } else {
            super.writeSubject(message, out);
        }

    }
}
