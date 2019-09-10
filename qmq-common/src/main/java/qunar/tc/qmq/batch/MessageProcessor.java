package qunar.tc.qmq.batch;

import qunar.tc.qmq.ProduceMessage;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-08-20
 */
public interface MessageProcessor {

    void process(List<ProduceMessage> items, OrderedSendMessageExecutor executor);
}
