package qunar.tc.qmq.producer.sender;

import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.service.exceptions.MessageException;

import java.util.List;
import java.util.Map;

/**
 * @author zhenwei.liu
 * @since 2019-08-20
 */
public interface SendCallback {

    /**
     * 发送消息完成后执行的回调
     *
     * @param source 发送的消息
     * @param result 消息发送结果
     */
    void run(List<ProduceMessage> source, Map<String, MessageException> result);
}
