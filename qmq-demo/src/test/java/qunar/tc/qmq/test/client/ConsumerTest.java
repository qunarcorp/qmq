package qunar.tc.qmq.test.client;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static qunar.tc.qmq.test.client.MessageTestUtils.BEST_TRIED_MESSAGE_SUBJECT;
import static qunar.tc.qmq.test.client.MessageTestUtils.SHARED_CONSUMER_GROUP;
import static qunar.tc.qmq.test.client.MessageTestUtils.bestTriedGenerateMessageFile;
import static qunar.tc.qmq.test.client.MessageTestUtils.checkSharedBestTriedConsumeMessageFile;
import static qunar.tc.qmq.test.client.MessageTestUtils.getClientId;
import static qunar.tc.qmq.test.client.MessageTestUtils.replayMessages;
import static qunar.tc.qmq.test.client.MessageTestUtils.saveMessage;
import static qunar.tc.qmq.test.client.MessageTestUtils.setClientId;
import static qunar.tc.qmq.test.client.MessageTestUtils.sharedBestTriedConsumerMessageFile;
import static qunar.tc.qmq.test.client.MessageTestUtils.sharedStrictConsumerMessageFile;

import com.google.common.collect.Maps;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
    public void testSharedBestTriedConsumeSuccess() throws Exception {
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
    public void testSharedBestTriedConsumeException() throws Exception {
        testSendBestTriedMessages();

        SubscribeParam param = new SubscribeParam.SubscribeParamBuilder().create();
        PrintWriter pw = new PrintWriter(new FileWriter(sharedBestTriedConsumerMessageFile));

        // messageId -> consumptionCount
        Map<String, AtomicInteger> exceptionMessageMap = Maps.newConcurrentMap();

        final ListenerHolder listener = consumerProvider.addListener(BEST_TRIED_MESSAGE_SUBJECT, SHARED_CONSUMER_GROUP,
                msg -> {
                    // 消费异常, best tried 模式会跳过该消息, 并被 ACK 回去, 代表后面会受到 %retry% 消息
                    String messageId = msg.getMessageId();
                    AtomicInteger counter = exceptionMessageMap
                            .computeIfAbsent(messageId, k -> new AtomicInteger());
                    if (counter.get() == 0 && shouldThrowException(msg)) {
                        counter.incrementAndGet();
                        LOGGER.info("message process error msg: {}", messageId);
                        // retry 至少会延迟 5s
                        renewWaitTimestamp(15);
                        throw new RuntimeException("mock exception");
                    } else {
                        LOGGER.info("message process successfully msg: {}", messageId);
                        saveMessage(msg, pw);
                        renewWaitTimestamp(15);
                    }
                }, executor, param);

        waitUntilNoNewMessage();
        listener.stopListen();
        consumerProvider.destroy();
        pw.close();

        checkSharedBestTriedConsumeMessageFile(bestTriedGenerateMessageFile, sharedBestTriedConsumerMessageFile);

        List<Message> savedMessages = replayMessages(sharedBestTriedConsumerMessageFile);
        for (Message savedMessage : savedMessages) {
            if (shouldThrowException(savedMessage)) {
                String messageId = savedMessage.getMessageId();
                AtomicInteger exCounter = exceptionMessageMap.get(messageId);
                assertNotNull(exCounter);
                assertEquals(exCounter.get(), 1);
            }
        }
    }

    private boolean shouldThrowException(Message message) {
        return message.getMessageId().hashCode() % 10 == 0;
    }

    @Test
    public void testSharedStrictConsume() throws Exception {
        testSendStrictMessages();

        SubscribeParam param = new SubscribeParam.SubscribeParamBuilder()
                .setOrdered(true)
                .create();
        PrintWriter pw = new PrintWriter(new FileWriter(sharedStrictConsumerMessageFile));
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

    /**
     * 持续消费, 直到 1s 没有新消息出现
     */
    private void waitUntilNoNewMessage() throws InterruptedException {
        while (System.currentTimeMillis() < waitTimestamp) {
            Thread.sleep(1000);
        }
    }

    private void renewWaitTimestamp() {
        renewWaitTimestamp(2);
    }

    private void renewWaitTimestamp(int seconds) {
        waitTimestamp = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(seconds);
    }
}
