package qunar.tc.qmq.test.client;

import static qunar.tc.qmq.test.client.MessageTestManager.BEST_TRIED_MESSAGE_SUBJECT;
import static qunar.tc.qmq.test.client.MessageTestManager.SHARED_CONSUMER_GROUP;
import static qunar.tc.qmq.test.client.MessageTestManager.STRICT_MESSAGE_SUBJECT;
import static qunar.tc.qmq.test.client.MessageTestManager.bestTriedGenerateMessageFile;
import static qunar.tc.qmq.test.client.MessageTestManager.checkSharedBestTriedConsumeMessageFile;
import static qunar.tc.qmq.test.client.MessageTestManager.getClientId;
import static qunar.tc.qmq.test.client.MessageTestManager.replayMessages;
import static qunar.tc.qmq.test.client.MessageTestManager.saveMessage;
import static qunar.tc.qmq.test.client.MessageTestManager.setClientId;
import static qunar.tc.qmq.test.client.MessageTestManager.sharedBestTriedConsumerMessageFile;
import static qunar.tc.qmq.test.client.MessageTestManager.sharedStrictConsumerMessageFile;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ListenerHolder;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.SubscribeParam;
import qunar.tc.qmq.consumer.MessageConsumerProvider;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019/6/3
 */
public class ConsumerTest extends ProducerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerTest.class);
    private static final ExecutorService executor = Executors.newFixedThreadPool(3);

    private static MessageConsumerProvider consumerProvider;

    @BeforeClass
    public static void initConsumer() {
        String clientId = getClientId();
        if (clientId == null) {
//            setClientId(ManagementFactory.getRuntimeMXBean().getName());
            setClientId("test-consumer1");
        }
        consumerProvider = new MessageConsumerProvider();
        consumerProvider.setMetaServer("http://127.0.0.1:8080/meta/address");
        consumerProvider.setAppCode("consumer_test");
        consumerProvider.setClientIdProvider(new TestClientIdProvider());
        consumerProvider.init();
        consumerProvider.online();
    }

    private long waitTimestamp = Long.MAX_VALUE;

    @Test
    public void testSharedBestTriedConsume() throws Exception {
        testSendBestTriedMessages();

        SubscribeParam param = new SubscribeParam.SubscribeParamBuilder().create();
        PrintWriter pw = new PrintWriter(new FileWriter(sharedBestTriedConsumerMessageFile));
        final ListenerHolder listener = consumerProvider.addListener(BEST_TRIED_MESSAGE_SUBJECT, SHARED_CONSUMER_GROUP,
                msg -> {
                    saveMessage(msg, pw);
                    renewWaitTimestamp();
                }, executor, param);

        waitUntilNoNewMessage();
        listener.stopListen();
        consumerProvider.destroy();
        pw.close();

        checkSharedBestTriedConsumeMessageFile(bestTriedGenerateMessageFile, sharedBestTriedConsumerMessageFile);
    }

    @Test
    public void testSharedStrictConsume() throws Exception {
        SubscribeParam param = new SubscribeParam.SubscribeParamBuilder().create();
        PrintWriter pw = new PrintWriter(new FileWriter(sharedStrictConsumerMessageFile));
        final ListenerHolder listener = consumerProvider.addListener(STRICT_MESSAGE_SUBJECT, SHARED_CONSUMER_GROUP,
                msg -> saveMessage(msg, pw), executor, param);

        System.in.read();
        listener.stopListen();
        consumerProvider.destroy();
        pw.close();
    }

    /**
     * 持续消费, 直到 1s 没有新消息出现
     */
    private void waitUntilNoNewMessage() throws InterruptedException {
        while(System.currentTimeMillis() < waitTimestamp) {
            Thread.sleep(1000);
        }
    }

    private void renewWaitTimestamp() {
        waitTimestamp = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(2);
    }
}
