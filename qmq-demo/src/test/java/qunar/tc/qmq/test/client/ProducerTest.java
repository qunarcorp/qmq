package qunar.tc.qmq.test.client;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.List;
import java.util.concurrent.CountDownLatch;
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
    public void testSendBestTriedMessages() throws Exception {
        List<Message> messages = MessageTestManager
                .generateMessages(producerProvider, MessageTestManager.BEST_TRIED_MESSAGE_SUBJECT, 100);
        MessageTestManager.saveMessage(messages, MessageTestManager.bestTriedGenerateMessageFile);
        try (PrintWriter writer = new PrintWriter(
                new BufferedWriter(new FileWriter(MessageTestManager.bestTriedSendMessageFile)))) {
            sendMessage(producerProvider, messages, writer);
        }
        MessageTestManager.checkBestTriedSendMessageFile(MessageTestManager.bestTriedGenerateMessageFile,
                MessageTestManager.bestTriedSendMessageFile);
    }

    @Test
    public void testSendStrictMessages() throws Exception {
        List<Message> messages = MessageTestManager
                .generateMessages(producerProvider, MessageTestManager.STRICT_MESSAGE_SUBJECT, 100);
        OrderStrategyManager.setOrderStrategy(MessageTestManager.STRICT_MESSAGE_SUBJECT, OrderStrategy.STRICT);
        MessageTestManager.saveMessage(messages, MessageTestManager.strictGenerateMessageFile);
        try (PrintWriter writer = new PrintWriter(
                new BufferedWriter(new FileWriter(MessageTestManager.strictSendMessageFile)))) {
            sendMessage(producerProvider, messages, writer);
        }
        MessageTestManager.checkStrictSendMessageFile(MessageTestManager.strictGenerateMessageFile,
                MessageTestManager.strictSendMessageFile);
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
                        MessageTestManager.saveMessage(message, writer);
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
