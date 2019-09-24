package qunar.tc.qmq.producer;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ListenerHolder;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.MessageListener;
import qunar.tc.qmq.consumer.MessageConsumerProvider;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019/6/3
 */
public class ConsumerTest {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerTest.class);
    private static final ExecutorService executor = Executors.newFixedThreadPool(3);

    @Test
    public void testConsumeMessage() throws Exception {
        final MessageConsumerProvider provider = new MessageConsumerProvider();
        provider.setMetaServer("http://127.0.0.1:8080/meta/address");
        provider.setAppCode("consumer_test");
        provider.init();

        final ListenerHolder listener = provider.addListener("new.qmq.test", "consumer_group1", new MessageListener() {
            @Override
            public void onMessage(Message msg) {
                logger.info("msgId:{}", msg.getMessageId());
            }
        }, executor);

        System.in.read();
        listener.stopListen();
        provider.destroy();
        TimeUnit.SECONDS.sleep(5);
    }
}
