package qunar.tc.qmq.producer.sender;

import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.base.BaseMessage.keys;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.common.OrderedMessageUtils;
import qunar.tc.qmq.meta.PartitionMapping;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-08-20
 */
public class OrderedSendMessagePreHandler implements SendMessagePreHandler {

    private BrokerService brokerService;

    @Override
    public void handle(List<ProduceMessage> messages) {
        for (ProduceMessage message : messages) {
            BaseMessage baseMessage = (BaseMessage) message.getBase();
            String subject = baseMessage.getSubject();

            // 顺序主题必须设置 order key 再发送
            PartitionMapping partitionMapping = brokerService.getPartitionMapping(subject);
            if (partitionMapping != null) {

            }

            baseMessage.removeProperty(keys.qmq_queueSenderType);
            baseMessage.removeProperty(keys.qmq_loadBalanceType);
        }
    }

    @Override
    public void init(BrokerService brokerService) {
        this.brokerService = brokerService;
    }
}
