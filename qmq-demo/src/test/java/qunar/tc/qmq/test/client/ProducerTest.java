package qunar.tc.qmq.test.client;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.MessageSendStateListener;
import qunar.tc.qmq.common.OrderStrategy;
import qunar.tc.qmq.common.OrderStrategyManager;
import qunar.tc.qmq.producer.MessageProducerProvider;
import qunar.tc.qmq.producer.tx.spring.SpringTransactionProvider;

public class ProducerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerTest.class);
    private static DataSource dataSource;
    private static MessageProducerProvider producerProvider;

    @BeforeClass
    public static void initProducer() throws Exception {
        dataSource = new DataSource();
        dataSource.setUrl("jdbc:mysql://127.0.0.1:3306/qmq_produce");
        dataSource.setUsername("root");
        dataSource.setPassword("root");

        producerProvider = new MessageProducerProvider();
        producerProvider.setAppCode("producer_test");
        producerProvider.setMetaServer("http://127.0.0.1:8080/meta/address");
        producerProvider.setTransactionProvider(new SpringTransactionProvider(dataSource));
        producerProvider.init();
    }

    @Test
    public void testSendSharedBestTriedMessages() throws Exception {
        List<Message> messages = MessageTestUtils
                .generateMessages(producerProvider, MessageTestUtils.SHARED_BEST_TRIED_MESSAGE_SUBJECT, 100);
        MessageTestUtils.saveMessage(messages, MessageTestUtils.bestTriedGenerateMessageFile);
        try (PrintWriter writer = new PrintWriter(
                new BufferedWriter(new FileWriter(MessageTestUtils.bestTriedSendMessageFile)))) {
            sendMessage(producerProvider, messages, writer);
        }
        MessageTestUtils.checkBestTriedSendMessageFile(MessageTestUtils.bestTriedGenerateMessageFile,
                MessageTestUtils.bestTriedSendMessageFile);
    }

    @Test
    public void testSendSharedStrictMessages() throws Exception {
        List<Message> messages = MessageTestUtils
                .generateMessages(producerProvider, MessageTestUtils.SHARED_STRICT_MESSAGE_SUBJECT, 100);
        OrderStrategyManager.setOrderStrategy(MessageTestUtils.SHARED_STRICT_MESSAGE_SUBJECT, OrderStrategy.STRICT);
        MessageTestUtils.saveMessage(messages, MessageTestUtils.strictGenerateMessageFile);
        try (PrintWriter writer = new PrintWriter(
                new BufferedWriter(new FileWriter(MessageTestUtils.strictSendMessageFile)))) {
            sendMessage(producerProvider, messages, writer);
        }
        MessageTestUtils.checkStrictSendMessageFile(MessageTestUtils.strictGenerateMessageFile,
                MessageTestUtils.strictSendMessageFile);
    }

    @Test
    public void testSendOrderedBestTriedMessages() throws Exception {
        List<Message> messages = MessageTestUtils
                .generateMessages(producerProvider, MessageTestUtils.EXCLUSIVE_BEST_TRIED_MESSAGE_SUBJECT, 100);
        for (Message message : messages) {
            message.setOrderKey(MessageTestUtils.getOrderKey(message));
        }
        MessageTestUtils.saveMessage(messages, MessageTestUtils.bestTriedGenerateMessageFile);
        try (PrintWriter writer = new PrintWriter(
                new BufferedWriter(new FileWriter(MessageTestUtils.bestTriedSendMessageFile)))) {
            sendMessage(producerProvider, messages, writer);
        }
        MessageTestUtils.checkBestTriedSendMessageFile(MessageTestUtils.bestTriedGenerateMessageFile,
                MessageTestUtils.bestTriedSendMessageFile);
    }

    @Test
    public void testSendOrderedStrictMessages() throws Exception {
        List<Message> messages = MessageTestUtils
                .generateMessages(producerProvider, MessageTestUtils.EXCLUSIVE_STRICT_MESSAGE_SUBJECT, 100);
        for (Message message : messages) {
            message.setOrderKey(MessageTestUtils.getOrderKey(message));
        }
        OrderStrategyManager.setOrderStrategy(MessageTestUtils.EXCLUSIVE_STRICT_MESSAGE_SUBJECT, OrderStrategy.STRICT);
        MessageTestUtils.saveMessage(messages, MessageTestUtils.strictGenerateMessageFile);
        try (PrintWriter writer = new PrintWriter(
                new BufferedWriter(new FileWriter(MessageTestUtils.strictSendMessageFile)))) {
            sendMessage(producerProvider, messages, writer);
        }
        MessageTestUtils.checkStrictSendMessageFile(MessageTestUtils.strictGenerateMessageFile,
                MessageTestUtils.strictSendMessageFile);
    }

    @Test
    public void testSendDelayMessages() throws Exception {
        int messageCount = 2;
        List<Message> messages = MessageTestUtils
                .generateMessages(producerProvider, MessageTestUtils.DELAY_MESSAGE_SUBJECT, messageCount);
        for (Message message : messages) {
            message.setOrderKey(MessageTestUtils.getOrderKey(message));
            message.setDelayTime(5, TimeUnit.SECONDS);
        }
        OrderStrategyManager.setOrderStrategy(MessageTestUtils.DELAY_MESSAGE_SUBJECT, OrderStrategy.STRICT);
        MessageTestUtils.saveMessage(messages, MessageTestUtils.delayGenerateMessageFile);
        try (PrintWriter writer = new PrintWriter(
                new BufferedWriter(new FileWriter(MessageTestUtils.delaySendMessageFile)))) {
            sendMessage(producerProvider, messages, writer);
        }
        MessageTestUtils.checkStrictSendMessageFile(MessageTestUtils.delayGenerateMessageFile,
                MessageTestUtils.delaySendMessageFile);
    }

    private static void sendMessage(MessageProducerProvider provider, List<Message> messages, PrintWriter writer)
            throws InterruptedException {
        int size = messages.size();
        CountDownLatch countDownLatch = new CountDownLatch(size);
        for (Message message : messages) {
            provider.sendMessage(message, new MessageSendStateListener() {
                @Override
                public void onSuccess(Message message) {
                    if (writer != null) {
                        MessageTestUtils.saveMessage(message, writer);
                    }
                    LOGGER.info("send message success {}", message.getMessageId());
                    countDownLatch.countDown();
                }

                @Override
                public void onFailed(Message message) {
                    LOGGER.error("send message fail {}", message.getMessageId());
                    countDownLatch.countDown();
                }
            });
        }
        countDownLatch.await();
    }
}
