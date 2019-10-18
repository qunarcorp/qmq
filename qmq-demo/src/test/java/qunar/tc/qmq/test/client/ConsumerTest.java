package qunar.tc.qmq.test.client;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static qunar.tc.qmq.test.client.MessageTestUtils.DELAY_MESSAGE_SUBJECT;
import static qunar.tc.qmq.test.client.MessageTestUtils.EXCLUSIVE_BEST_TRIED_MESSAGE_SUBJECT;
import static qunar.tc.qmq.test.client.MessageTestUtils.EXCLUSIVE_CONSUMER_GROUP;
import static qunar.tc.qmq.test.client.MessageTestUtils.EXCLUSIVE_STRICT_MESSAGE_SUBJECT;
import static qunar.tc.qmq.test.client.MessageTestUtils.SHARED_BEST_TRIED_MESSAGE_SUBJECT;
import static qunar.tc.qmq.test.client.MessageTestUtils.SHARED_CONSUMER_GROUP;
import static qunar.tc.qmq.test.client.MessageTestUtils.SHARED_STRICT_MESSAGE_SUBJECT;
import static qunar.tc.qmq.test.client.MessageTestUtils.bestTriedGenerateMessageFile;
import static qunar.tc.qmq.test.client.MessageTestUtils.checkExclusiveBestTriedConsumeMessageExceptionFile;
import static qunar.tc.qmq.test.client.MessageTestUtils.checkExclusiveBestTriedConsumeMessageFile;
import static qunar.tc.qmq.test.client.MessageTestUtils.checkExclusiveStrictConsumeMessageFile;
import static qunar.tc.qmq.test.client.MessageTestUtils.checkSharedBestTriedConsumeMessageFile;
import static qunar.tc.qmq.test.client.MessageTestUtils.checkSharedStrictConsumeMessageFile;
import static qunar.tc.qmq.test.client.MessageTestUtils.delayGenerateMessageFile;
import static qunar.tc.qmq.test.client.MessageTestUtils.exclusiveBestTriedConsumerMessageFile;
import static qunar.tc.qmq.test.client.MessageTestUtils.exclusiveStrictConsumerMessageFile;
import static qunar.tc.qmq.test.client.MessageTestUtils.replayMessages;
import static qunar.tc.qmq.test.client.MessageTestUtils.saveMessage;
import static qunar.tc.qmq.test.client.MessageTestUtils.setClientId;
import static qunar.tc.qmq.test.client.MessageTestUtils.sharedBestTriedConsumerMessageFile;
import static qunar.tc.qmq.test.client.MessageTestUtils.sharedStrictConsumerMessageFile;
import static qunar.tc.qmq.test.client.MessageTestUtils.strictGenerateMessageFile;

import com.google.common.collect.Maps;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ListenerHolder;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.NeedRetryException;
import qunar.tc.qmq.SubscribeParam;
import qunar.tc.qmq.common.OrderStrategy;
import qunar.tc.qmq.common.OrderStrategyManager;
import qunar.tc.qmq.consumer.MessageConsumerProvider;
import qunar.tc.qmq.utils.RetryPartitionUtils;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019/6/3
 */
public class ConsumerTest extends ProducerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerTest.class);
    private static final ExecutorService executor = Executors.newFixedThreadPool(3);

    public MessageConsumerProvider initConsumer(String clinetId) {
        setClientId(clinetId);
        MessageConsumerProvider consumerProvider = new MessageConsumerProvider();
        consumerProvider.setMetaServer("http://127.0.0.1:8080/meta/address");
        consumerProvider.setAppCode("consumer_test");
        consumerProvider.setClientIdProvider(new TestClientIdProvider());
        consumerProvider.init();
        consumerProvider.online();
        return consumerProvider;
    }

    private long waitTimestamp = Long.MAX_VALUE;

    @Test
    public void testSharedBestTriedConsumeSuccess() throws Exception {
        MessageConsumerProvider consumerProvider = initConsumer("shared-best-tried-consumer-1");
        testSendSharedBestTriedMessages();

        SubscribeParam param = new SubscribeParam.SubscribeParamBuilder().create();
        PrintWriter pw = new PrintWriter(new FileWriter(sharedBestTriedConsumerMessageFile));
        final ListenerHolder listener = consumerProvider
                .addListener(SHARED_BEST_TRIED_MESSAGE_SUBJECT, SHARED_CONSUMER_GROUP,
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
        MessageConsumerProvider consumerProvider = initConsumer("shared-best-tried-consumer-1");
        testSendSharedBestTriedMessages();

        SubscribeParam param = new SubscribeParam.SubscribeParamBuilder().create();
        PrintWriter pw = new PrintWriter(new FileWriter(sharedBestTriedConsumerMessageFile));

        // messageId -> consumptionCount
        Map<String, AtomicInteger> exceptionMessageMap = Maps.newConcurrentMap();

        final ListenerHolder listener = consumerProvider
                .addListener(SHARED_BEST_TRIED_MESSAGE_SUBJECT, SHARED_CONSUMER_GROUP,
                        msg -> {
                            // 消费异常, best tried 模式会跳过该消息, 并被 ACK 回去, 代表后面会受到 %retry% 消息
                            String messageId = msg.getMessageId();
                            AtomicInteger counter = exceptionMessageMap
                                    .computeIfAbsent(messageId, k -> new AtomicInteger());
                            if (counter.get() == 0 && isChosenMessage(msg)) {
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
            if (isChosenMessage(savedMessage)) {
                String messageId = savedMessage.getMessageId();
                AtomicInteger exCounter = exceptionMessageMap.get(messageId);
                assertNotNull(exCounter);
                assertEquals(exCounter.get(), 1);
            }
        }
    }

    /**
     * 模拟消费抛出 NeedRetryException 场景, 3次本地重试后异常抛出, 进入 %retry% 流程
     */
    @Test
    public void testSharedBestTriedConsumeThrowNeedRetryException() throws Exception {
        MessageConsumerProvider consumerProvider = initConsumer("shared-best-tried-consumer-1");
        testSendSharedBestTriedMessages();

        SubscribeParam param = new SubscribeParam.SubscribeParamBuilder().create();
        PrintWriter pw = new PrintWriter(new FileWriter(sharedBestTriedConsumerMessageFile));

        Map<String, AtomicInteger> exceptionMessageMap = Maps.newConcurrentMap();

        final ListenerHolder listener = consumerProvider
                .addListener(SHARED_BEST_TRIED_MESSAGE_SUBJECT, SHARED_CONSUMER_GROUP,
                        msg -> {
                            String messageId = msg.getMessageId();
                            String partitionName = msg.getPartitionName();
                            int localRetries = msg.localRetries();
                            AtomicInteger counter = exceptionMessageMap
                                    .computeIfAbsent(messageId, k -> new AtomicInteger());
                            counter.incrementAndGet();
                            if (RetryPartitionUtils.isRetryPartitionName(partitionName)) {
                                LOGGER.info("message process successfully msg {} localRetries {}", messageId,
                                        localRetries);
                                saveMessage(msg, pw);
                                renewWaitTimestamp(15);
                            } else {
                                if (localRetries >= 3) {
                                    throw new RuntimeException("trigger retry");
                                }
                                LOGGER.info("message process error msg {} localRetries {}", messageId, localRetries);
                                // retry 至少会延迟 5s
                                renewWaitTimestamp(15);
                                throw new NeedRetryException("mock exception");
                            }
                        }, executor, param);

        waitUntilNoNewMessage();
        listener.stopListen();
        consumerProvider.destroy();
        pw.close();

        checkSharedBestTriedConsumeMessageFile(bestTriedGenerateMessageFile, sharedBestTriedConsumerMessageFile);

        List<Message> savedMessages = replayMessages(sharedBestTriedConsumerMessageFile);
        for (Message savedMessage : savedMessages) {
            if (isChosenMessage(savedMessage)) {
                String messageId = savedMessage.getMessageId();
                AtomicInteger exCounter = exceptionMessageMap.get(messageId);
                assertNotNull(exCounter);
                assertEquals(exCounter.get(), 5);
            }
        }
    }

    @Test
    public void testSharedBestTriedConsumeNoAck1() throws Exception {
        MessageConsumerProvider consumerProvider = initConsumer("shared-best-tried-consumer-1");
        testSendSharedBestTriedMessages();

        SubscribeParam param = new SubscribeParam.SubscribeParamBuilder().create();
        PrintWriter pw = new PrintWriter(new FileWriter(sharedBestTriedConsumerMessageFile));
        final ListenerHolder listener = consumerProvider
                .addListener(SHARED_BEST_TRIED_MESSAGE_SUBJECT, SHARED_CONSUMER_GROUP,
                        msg -> {
                            // 不自动 ack, 直到收到 retry 消息.
                            msg.autoAck(false);
                            String messageId = msg.getMessageId();
                            // 不 ack 消息实际上是 bug, 只有等到客户端连接断开, 才能出发 broker 的 retry 逻辑
                            LOGGER.info("message process not ack msg: {}", messageId);
                            // retry 至少会延迟 5s
                            saveMessage(msg, pw);
                            renewWaitTimestamp(5);
                        }, executor, param);

        waitUntilNoNewMessage();
        listener.stopListen();
        consumerProvider.destroy();
        pw.close();
    }

    /**
     * 用于测试 broker 连接断开后未 ack 的消息是否能再次收到
     */
    @Test
    public void testSharedBestTriedConsumeNoAck2() throws Exception {
        MessageConsumerProvider consumerProvider = initConsumer("shared-best-tried-consumer-1");
        SubscribeParam param = new SubscribeParam.SubscribeParamBuilder().create();
        List<Message> lastMessages = replayMessages(sharedBestTriedConsumerMessageFile);
        PrintWriter pw = new PrintWriter(new FileWriter(sharedBestTriedConsumerMessageFile));

        final ListenerHolder listener = consumerProvider
                .addListener(SHARED_BEST_TRIED_MESSAGE_SUBJECT, SHARED_CONSUMER_GROUP,
                        msg -> {
                            String messageId = msg.getMessageId();
                            LOGGER.info("message process ack msg: {}", messageId);
                            saveMessage(msg, pw);
                            // retry 至少会延迟 5s
                            renewWaitTimestamp(5);
                        }, executor, param);

        waitUntilNoNewMessage();
        listener.stopListen();
        consumerProvider.destroy();
        pw.close();

        List<Message> currentMessages = replayMessages(sharedBestTriedConsumerMessageFile);

        assertEquals(lastMessages.size(), currentMessages.size());
        assertEquals(
                lastMessages.stream().map(Message::getMessageId).collect(Collectors.toSet()),
                currentMessages.stream().map(Message::getMessageId).collect(Collectors.toSet())
        );
    }

    private boolean isChosenMessage(Message message) {
        return message.getMessageId().hashCode() % 10 == 0;
    }

    @Test
    public void testSharedStrictConsume() throws Exception {
        MessageConsumerProvider consumerProvider = initConsumer("shared-strict-consumer-0");
        testSendSharedStrictMessages();
        OrderStrategyManager.setOrderStrategy(SHARED_STRICT_MESSAGE_SUBJECT, OrderStrategy.STRICT);

        SubscribeParam param = new SubscribeParam.SubscribeParamBuilder()
                .create();
        PrintWriter pw = new PrintWriter(new FileWriter(sharedStrictConsumerMessageFile));
        final ListenerHolder listener = consumerProvider
                .addListener(SHARED_STRICT_MESSAGE_SUBJECT, SHARED_CONSUMER_GROUP,
                        msg -> {
                            saveMessage(msg, pw);
                            renewWaitTimestamp();
                        }, executor, param);

        waitUntilNoNewMessage();
        listener.stopListen();
        consumerProvider.destroy();
        pw.close();

        checkSharedStrictConsumeMessageFile(strictGenerateMessageFile, sharedStrictConsumerMessageFile);
    }

    @Test
    public void testSharedStrictConsumeException() throws Exception {
        MessageConsumerProvider consumerProvider = initConsumer("shared-strict-consumer-0");
        testSendSharedStrictMessages();
        OrderStrategyManager.setOrderStrategy(SHARED_STRICT_MESSAGE_SUBJECT, OrderStrategy.STRICT);

        SubscribeParam param = new SubscribeParam.SubscribeParamBuilder().create();
        PrintWriter pw = new PrintWriter(new FileWriter(sharedStrictConsumerMessageFile));

        // messageId -> consumptionCount
        Map<String, AtomicInteger> exceptionMessageMap = Maps.newConcurrentMap();

        final ListenerHolder listener = consumerProvider
                .addListener(SHARED_STRICT_MESSAGE_SUBJECT, SHARED_CONSUMER_GROUP,
                        msg -> {
                            String messageId = msg.getMessageId();
                            AtomicInteger counter = exceptionMessageMap
                                    .computeIfAbsent(messageId, k -> new AtomicInteger());
                            if (counter.get() == 0 && isChosenMessage(msg)) {
                                // 在严格模式下, 会进行本地重试, 而不是使用 %retry%
                                counter.incrementAndGet();
                                LOGGER.info("message process error msg: {}", messageId);
                                // retry 至少会延迟 5s
                                renewWaitTimestamp(5);
                                throw new RuntimeException("mock exception");
                            } else {
                                LOGGER.info("message process successfully msg: {}", messageId);
                                saveMessage(msg, pw);
                                renewWaitTimestamp(5);
                            }
                        }, executor, param);

        waitUntilNoNewMessage();
        listener.stopListen();
        consumerProvider.destroy();
        pw.close();

        checkSharedStrictConsumeMessageFile(strictGenerateMessageFile, sharedStrictConsumerMessageFile);

        List<Message> savedMessages = replayMessages(sharedStrictConsumerMessageFile);
        for (Message savedMessage : savedMessages) {
            if (isChosenMessage(savedMessage)) {
                String messageId = savedMessage.getMessageId();
                AtomicInteger exCounter = exceptionMessageMap.get(messageId);
                assertNotNull(exCounter);
                assertEquals(exCounter.get(), 1);
            }
        }
    }

    @Test
    public void testSharedStrictConsumeNotAck() throws Exception {
        MessageConsumerProvider consumerProvider = initConsumer("shared-strict-consumer-0");
        testSendSharedStrictMessages();
        OrderStrategyManager.setOrderStrategy(SHARED_STRICT_MESSAGE_SUBJECT, OrderStrategy.STRICT);

        SubscribeParam param = new SubscribeParam.SubscribeParamBuilder().create();
        PrintWriter pw = new PrintWriter(new FileWriter(sharedStrictConsumerMessageFile));

        // messageId -> consumptionCount
        Map<String, AtomicInteger> exceptionMessageMap = Maps.newConcurrentMap();

        final ListenerHolder listener = consumerProvider
                .addListener(SHARED_STRICT_MESSAGE_SUBJECT, SHARED_CONSUMER_GROUP,
                        msg -> {
                            String messageId = msg.getMessageId();
                            AtomicInteger counter = exceptionMessageMap
                                    .computeIfAbsent(messageId, k -> new AtomicInteger());
                            if (counter.get() == 0 && isChosenMessage(msg)) {
                                // 在严格模式下, 未 ACK 消息会进行本地重试, 而不是使用 %retry%
                                msg.autoAck(false);
                                counter.incrementAndGet();
                                LOGGER.info("message process error msg: {}", messageId);
                                // retry 至少会延迟 5s
                                renewWaitTimestamp(5);
                            } else {
                                msg.autoAck(true);
                                LOGGER.info("message process successfully msg: {}", messageId);
                                saveMessage(msg, pw);
                                renewWaitTimestamp(5);
                            }
                        }, executor, param);

        waitUntilNoNewMessage();
        listener.stopListen();
        consumerProvider.destroy();
        pw.close();

        checkSharedStrictConsumeMessageFile(strictGenerateMessageFile, sharedStrictConsumerMessageFile);

        List<Message> savedMessages = replayMessages(sharedStrictConsumerMessageFile);
        for (Message savedMessage : savedMessages) {
            if (isChosenMessage(savedMessage)) {
                String messageId = savedMessage.getMessageId();
                AtomicInteger exCounter = exceptionMessageMap.get(messageId);
                assertNotNull(exCounter);
                assertEquals(exCounter.get(), 1);
            }
        }
    }

    @Test
    public void testExclusiveBestTriedConsumeSuccess() throws Exception {
        MessageConsumerProvider consumerProvider = initConsumer("exclusive-best-tried-consumer-0");
        testSendOrderedBestTriedMessages();

        SubscribeParam param = new SubscribeParam.SubscribeParamBuilder()
                .setOrdered(true)
                .create();
        PrintWriter pw = new PrintWriter(new FileWriter(exclusiveBestTriedConsumerMessageFile));
        final ListenerHolder listener = consumerProvider
                .addListener(EXCLUSIVE_BEST_TRIED_MESSAGE_SUBJECT, EXCLUSIVE_CONSUMER_GROUP,
                        msg -> {
                            saveMessage(msg, pw);
                            renewWaitTimestamp();
                        }, executor, param);

        waitUntilNoNewMessage();
        listener.stopListen();
        consumerProvider.destroy();
        pw.close();

        checkExclusiveBestTriedConsumeMessageFile(bestTriedGenerateMessageFile, exclusiveBestTriedConsumerMessageFile);
    }

    @Test
    public void testExclusiveBestTriedConsumeException() throws Exception {
        MessageConsumerProvider consumerProvider = initConsumer("exclusive-best-tried-consumer-0");
        testSendOrderedBestTriedMessages();

        SubscribeParam param = new SubscribeParam.SubscribeParamBuilder()
                .setOrdered(true)
                .create();
        PrintWriter pw = new PrintWriter(new FileWriter(exclusiveBestTriedConsumerMessageFile));

        // messageId -> consumptionCount
        Map<String, AtomicInteger> exceptionMessageMap = Maps.newConcurrentMap();

        final ListenerHolder listener = consumerProvider
                .addListener(EXCLUSIVE_BEST_TRIED_MESSAGE_SUBJECT, EXCLUSIVE_CONSUMER_GROUP,
                        msg -> {
                            // 消费异常, best tried 模式会使用 %retry%, 所以会出现部分乱序
                            String messageId = msg.getMessageId();
                            AtomicInteger counter = exceptionMessageMap
                                    .computeIfAbsent(messageId, k -> new AtomicInteger());
                            if (counter.get() == 0 && isChosenMessage(msg)) {
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

        checkExclusiveBestTriedConsumeMessageExceptionFile(bestTriedGenerateMessageFile, exclusiveBestTriedConsumerMessageFile);

        List<Message> savedMessages = replayMessages(exclusiveBestTriedConsumerMessageFile);
        for (Message savedMessage : savedMessages) {
            if (isChosenMessage(savedMessage)) {
                String messageId = savedMessage.getMessageId();
                AtomicInteger exCounter = exceptionMessageMap.get(messageId);
                assertNotNull(exCounter);
                assertEquals(exCounter.get(), 1);
            }
        }
    }

    /**
     * 测试消息没有 ACK 时的行为, best tried 模式下未 ack 消息在 client 断开后会进入 %retry% 阶段
     * 测试需要配合 testExclusiveBestTriedConsumeNoAck2() 执行最终的消息验证
     */
    @Test
    public void testExclusiveBestTriedConsumeNoAck1() throws Exception {
        MessageConsumerProvider consumerProvider = initConsumer("exclusive-best-tried-consumer-0");
        testSendOrderedBestTriedMessages();

        SubscribeParam param = new SubscribeParam.SubscribeParamBuilder()
                .setOrdered(true)
                .create();
        PrintWriter pw = new PrintWriter(new FileWriter(exclusiveBestTriedConsumerMessageFile));
        final ListenerHolder listener = consumerProvider
                .addListener(EXCLUSIVE_BEST_TRIED_MESSAGE_SUBJECT, EXCLUSIVE_CONSUMER_GROUP,
                        msg -> {
                            // 不自动 ack, 直到收到 retry 消息.
                            msg.autoAck(false);
                            String messageId = msg.getMessageId();
                            // 不 ack 消息实际上是 bug, 只有等到客户端连接断开, 才能出发 broker 的 retry 逻辑
                            LOGGER.info("message process not ack msg: {}", messageId);
                            // retry 至少会延迟 5s
                            saveMessage(msg, pw);
                            renewWaitTimestamp(5);
                        }, executor, param);

        waitUntilNoNewMessage();
        listener.stopListen();
        consumerProvider.destroy();
        pw.close();
    }

    /**
     * 用于测试 broker 连接断开后未 ack 的消息是否能再次收到
     */
    @Test
    public void testExclusiveBestTriedConsumeNoAck2() throws Exception {
        MessageConsumerProvider consumerProvider = initConsumer("exclusive-best-tried-consumer-0");
        SubscribeParam param = new SubscribeParam.SubscribeParamBuilder()
                .setOrdered(true)
                .create();
        List<Message> lastMessages = replayMessages(exclusiveBestTriedConsumerMessageFile);
        PrintWriter pw = new PrintWriter(new FileWriter(exclusiveBestTriedConsumerMessageFile));

        final ListenerHolder listener = consumerProvider
                .addListener(EXCLUSIVE_BEST_TRIED_MESSAGE_SUBJECT, EXCLUSIVE_CONSUMER_GROUP,
                        msg -> {
                            String messageId = msg.getMessageId();
                            LOGGER.info("message process ack msg: {}", messageId);
                            saveMessage(msg, pw);
                            renewWaitTimestamp(5);
                        }, executor, param);

        waitUntilNoNewMessage();
        listener.stopListen();
        consumerProvider.destroy();
        pw.close();

        List<Message> currentMessages = replayMessages(exclusiveBestTriedConsumerMessageFile);

        assertEquals(lastMessages.size(), currentMessages.size());
        assertEquals(
                lastMessages.stream().map(Message::getMessageId).collect(Collectors.toSet()),
                currentMessages.stream().map(Message::getMessageId).collect(Collectors.toSet())
        );
    }

    @Test
    public void testExclusiveStrictConsumeSuccess() throws Exception {
        MessageConsumerProvider consumerProvider = initConsumer("exclusive-strict-consumer-0");
        testSendOrderedStrictMessages();

        OrderStrategyManager.setOrderStrategy(EXCLUSIVE_STRICT_MESSAGE_SUBJECT, OrderStrategy.STRICT);
        SubscribeParam param = new SubscribeParam.SubscribeParamBuilder()
                .setOrdered(true)
                .create();
        PrintWriter pw = new PrintWriter(new FileWriter(exclusiveStrictConsumerMessageFile));
        final ListenerHolder listener = consumerProvider
                .addListener(EXCLUSIVE_STRICT_MESSAGE_SUBJECT, EXCLUSIVE_CONSUMER_GROUP,
                        msg -> {
                            saveMessage(msg, pw);
                            renewWaitTimestamp();
                        }, executor, param);

        waitUntilNoNewMessage();
        listener.stopListen();
        consumerProvider.destroy();
        pw.close();

        checkExclusiveStrictConsumeMessageFile(strictGenerateMessageFile, exclusiveStrictConsumerMessageFile);
    }

    @Test
    public void testExclusiveStrictConsumeException() throws Exception {
        MessageConsumerProvider consumerProvider = initConsumer("exclusive-strict-consumer-0");
        testSendOrderedStrictMessages();

        SubscribeParam param = new SubscribeParam.SubscribeParamBuilder()
                .setOrdered(true)
                .create();
        PrintWriter pw = new PrintWriter(new FileWriter(exclusiveStrictConsumerMessageFile));

        // messageId -> consumptionCount
        Map<String, AtomicInteger> exceptionMessageMap = Maps.newConcurrentMap();

        final ListenerHolder listener = consumerProvider
                .addListener(EXCLUSIVE_STRICT_MESSAGE_SUBJECT, EXCLUSIVE_CONSUMER_GROUP,
                        msg -> {
                            // 消费异常, strict 会使用本地重试, 且保证顺序一直
                            String messageId = msg.getMessageId();
                            AtomicInteger counter = exceptionMessageMap
                                    .computeIfAbsent(messageId, k -> new AtomicInteger());
                            if (counter.get() == 0 && isChosenMessage(msg)) {
                                counter.incrementAndGet();
                                LOGGER.info("message process error msg: {}", messageId);
                                // retry 至少会延迟 5s
                                renewWaitTimestamp(5);
                                throw new RuntimeException("mock exception");
                            } else {
                                LOGGER.info("message process successfully msg: {}", messageId);
                                saveMessage(msg, pw);
                                renewWaitTimestamp(5);
                            }
                        }, executor, param);

        waitUntilNoNewMessage();
        listener.stopListen();
        consumerProvider.destroy();
        pw.close();

        checkExclusiveStrictConsumeMessageFile(strictGenerateMessageFile, exclusiveStrictConsumerMessageFile);

        List<Message> savedMessages = replayMessages(exclusiveStrictConsumerMessageFile);
        for (Message savedMessage : savedMessages) {
            if (isChosenMessage(savedMessage)) {
                String messageId = savedMessage.getMessageId();
                AtomicInteger exCounter = exceptionMessageMap.get(messageId);
                assertNotNull(exCounter);
                assertEquals(exCounter.get(), 1);
            }
        }
    }

    @Test
    public void testExclusiveStrictConsumeNoAck() throws Exception {
        MessageConsumerProvider consumerProvider = initConsumer("exclusive-strict-consumer-0");
        testSendOrderedStrictMessages();

        SubscribeParam param = new SubscribeParam.SubscribeParamBuilder()
                .setOrdered(true)
                .create();
        PrintWriter pw = new PrintWriter(new FileWriter(exclusiveStrictConsumerMessageFile));

        // messageId -> consumptionCount
        Map<String, AtomicInteger> exceptionMessageMap = Maps.newConcurrentMap();

        final ListenerHolder listener = consumerProvider
                .addListener(EXCLUSIVE_STRICT_MESSAGE_SUBJECT, EXCLUSIVE_CONSUMER_GROUP,
                        msg -> {
                            // 消费异常, strict 会使用本地重试, 且保证顺序一直
                            String messageId = msg.getMessageId();
                            AtomicInteger counter = exceptionMessageMap
                                    .computeIfAbsent(messageId, k -> new AtomicInteger());
                            if (counter.get() == 0 && isChosenMessage(msg)) {
                                msg.autoAck(false);
                                counter.incrementAndGet();
                                LOGGER.info("message process error msg: {}", messageId);
                                // retry 至少会延迟 5s
                                renewWaitTimestamp(5);
                            } else {
                                msg.autoAck(true);
                                LOGGER.info("message process successfully msg: {}", messageId);
                                saveMessage(msg, pw);
                                renewWaitTimestamp(5);
                            }
                        }, executor, param);

        waitUntilNoNewMessage();
        listener.stopListen();
        consumerProvider.destroy();
        pw.close();

        checkExclusiveStrictConsumeMessageFile(strictGenerateMessageFile, exclusiveStrictConsumerMessageFile);

        List<Message> savedMessages = replayMessages(exclusiveStrictConsumerMessageFile);
        for (Message savedMessage : savedMessages) {
            if (isChosenMessage(savedMessage)) {
                String messageId = savedMessage.getMessageId();
                AtomicInteger exCounter = exceptionMessageMap.get(messageId);
                assertNotNull(exCounter);
                assertEquals(exCounter.get(), 1);
            }
        }
    }

    @Test
    public void testConsumeDelayMessages() throws Exception {
        MessageConsumerProvider consumerProvider = initConsumer("delay-shared-strict-consumer-0");
        testSendDelayMessages();

        SubscribeParam param = new SubscribeParam.SubscribeParamBuilder().create();
        PrintWriter pw = new PrintWriter(new FileWriter(sharedStrictConsumerMessageFile));
        final ListenerHolder listener = consumerProvider
                .addListener(DELAY_MESSAGE_SUBJECT, SHARED_CONSUMER_GROUP,
                        msg -> {
                            saveMessage(msg, pw);
                            renewWaitTimestamp(10);
                        }, executor, param);

        waitUntilNoNewMessage();
        listener.stopListen();
        consumerProvider.destroy();
        pw.close();

        checkSharedStrictConsumeMessageFile(delayGenerateMessageFile, sharedStrictConsumerMessageFile);
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
