package qunar.tc.qmq.test.client;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ListenerHolder;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.MessageListener;
import qunar.tc.qmq.SubscribeParam;
import qunar.tc.qmq.consumer.MessageConsumerProvider;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019/6/3
 */
public class ConsumerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerTest.class);
    private static final ExecutorService executor = Executors.newFixedThreadPool(3);

    @Test
    public void testConsumeMessage() throws Exception {
        final MessageConsumerProvider provider = new MessageConsumerProvider();
        provider.setMetaServer("http://127.0.0.1:8080/meta/address");
        provider.setAppCode("consumer_test");
        provider.setClientIdProvider(new TestClientIdProvider());
        provider.init();

        SubscribeParam param = new SubscribeParam.SubscribeParamBuilder()
                .setOrdered(true)
                .create();
        final ListenerHolder listener = provider.addListener("alloc.partition.subject", "test_consumer_group", new MessageListener() {
            @Override
            public void onMessage(Message msg) {
                LOGGER.info("msgId:{}", msg.getMessageId());
            }
        }, executor, param);

        provider.online();
        System.in.read();
        listener.stopListen();
        provider.destroy();
        TimeUnit.SECONDS.sleep(5);
    }
}
