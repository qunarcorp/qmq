package qunar.tc.qmq.producer.sender;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.base.BaseMessage.keys;
import qunar.tc.qmq.producer.QueueSender;

import java.util.Map;
import java.util.ServiceLoader;

/**
 * @author zhenwei.liu
 * @since 2019-08-20
 */
public class AdaptiveQueueSender implements QueueSender {

    private static final String DEFAULT_SENDER = RPCQueueSender.class.getName();
    private static final Logger logger = LoggerFactory.getLogger(AdaptiveQueueSender.class);
    private Map<String, QueueSender> senderMap = Maps.newHashMap();

    public AdaptiveQueueSender() {
        OrderedQueueSender orderedQueueSender = new OrderedQueueSender();
        RPCQueueSender rpcQueueSender = new RPCQueueSender();
        senderMap.put(orderedQueueSender.getClass().getName(), orderedQueueSender);
        senderMap.put(rpcQueueSender.getClass().getName(), rpcQueueSender);
    }

    @Override
    public void init(Map<PropKey, Object> props) {
        for (QueueSender sender : senderMap.values()) {
            sender.init(props);
        }
    }

    @Override
    public boolean offer(ProduceMessage pm) {
        return selectSender(pm).offer(pm);
    }

    @Override
    public boolean offer(ProduceMessage pm, long millisecondWait) {
        return selectSender(pm).offer(pm, millisecondWait);
    }

    @Override
    public void send(ProduceMessage pm) {
        selectSender(pm).send(pm);
    }

    @Override
    public void destroy() {
        for (QueueSender sender : senderMap.values()) {
            String name = sender.getClass().getName();
            try {
                logger.info("destroy sender {}", name);
                sender.destroy();
            } catch (Throwable t) {
                logger.error("sender {} destroy error", name, t);
            }
        }
    }

    private QueueSender selectSender(ProduceMessage message) {
        BaseMessage baseMessage = (BaseMessage) message.getBase();
        Object senderType = baseMessage.getProperty(keys.qmq_queueSenderType);
        if (senderType == null) {
            return senderMap.get(DEFAULT_SENDER);
        }
        return senderMap.get(senderType.toString());
    }
}
