package qunar.tc.qmq.producer.sender;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.batch.Processor;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.metrics.QmqTimer;
import qunar.tc.qmq.netty.exception.SubjectNotAssignedException;
import qunar.tc.qmq.producer.QueueSender;
import qunar.tc.qmq.producer.SendErrorHandler;
import qunar.tc.qmq.service.exceptions.MessageException;
import qunar.tc.qmq.tracing.TraceUtil;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author zhenwei.liu
 * @since 2019-08-20
 */
public abstract class AbstractQueueSender implements QueueSender, SendErrorHandler, Processor<ProduceMessage> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected RouterManager routerManager;
    protected QmqTimer timer;

    @Override
    public void process(List<ProduceMessage> produceMessages) {
        long start = System.currentTimeMillis();
        try {
            //按照路由分组发送
            Collection<MessageSenderGroup> messages = groupBy(produceMessages);
            for (MessageSenderGroup group : messages) {
                QmqTimer timer = Metrics.timer("qmq_client_producer_send_broker_time");
                long startTime = System.currentTimeMillis();
                group.send(null);
                timer.update(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
            }
        } finally {
            timer.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
        }
    }

    protected Collection<MessageSenderGroup> groupBy(List<ProduceMessage> list) {
        Map<Connection, MessageSenderGroup> map = Maps.newHashMap();
        for (int i = 0; i < list.size(); ++i) {
            ProduceMessage produceMessage = list.get(i);
            produceMessage.startSendTrace();
            Connection connection = routerManager.routeOf(produceMessage.getBase());
            MessageSenderGroup group = map.get(connection);
            if (group == null) {
                group = new MessageSenderGroup(this, connection);
                map.put(connection, group);
            }
            group.addMessage(produceMessage);
        }
        return map.values();
    }

    @Override
    public void error(ProduceMessage pm, Exception e) {
        if (!(e instanceof SubjectNotAssignedException)) {
            logger.warn("Message 发送失败! {}", pm.getMessageId(), e);
        }
        TraceUtil.recordEvent("error");
        pm.error(e);
    }

    @Override
    public void failed(ProduceMessage pm, Exception e) {
        logger.warn("Message 发送失败! {}", pm.getMessageId(), e);
        TraceUtil.recordEvent("failed ");
        pm.failed();
    }

    @Override
    public void block(ProduceMessage pm, MessageException ex) {
        logger.warn("Message 发送失败! {},被server拒绝,请检查应用授权配置,如果需要恢复消息请手工到db恢复状态", pm.getMessageId(), ex);
        TraceUtil.recordEvent("block");
        pm.block();
    }

    @Override
    public void finish(ProduceMessage pm, Exception e) {
        logger.info("发送成功 {}:{}", pm.getSubject(), pm.getMessageId());
        pm.finish();
    }
}
