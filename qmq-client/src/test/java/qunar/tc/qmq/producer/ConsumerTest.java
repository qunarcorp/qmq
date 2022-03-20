package qunar.tc.qmq.producer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ListenerHolder;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.MessageListener;
import qunar.tc.qmq.common.ClientInfo;
import qunar.tc.qmq.consumer.MessageConsumerProvider;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019/6/3
 */
public class ConsumerTest {
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerTest.class);

    private static final ExecutorService executor = Executors.newFixedThreadPool(3);

    public static void main(String[] args) throws Exception {
        final MessageConsumerProvider provider = new MessageConsumerProvider();
        provider.setMetaServer("http://127.0.0.1:8080/meta/address");
        provider.setAppCode("consumer_test");
        provider.setClientInfo(ClientInfo.of("consumer_test","ldc1"));
        provider.init();
        provider.online();

        final ListenerHolder listener = provider.addListener("new.qmq.test", "group1", new MessageListener() {
            @Override
            public void onMessage(Message msg) {
                LOG.info("msgId:{}", msg.getMessageId());
            }
        }, executor);

        System.in.read();
        listener.stopListen();
        provider.destroy();
    }
}
