package com.qunar.qmq.client;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.MessageListener;
import qunar.tc.qmq.MessageSendStateListener;
import qunar.tc.qmq.common.ClientIdProvider;
import qunar.tc.qmq.consumer.MessageConsumerProvider;
import qunar.tc.qmq.producer.MessageProducerProvider;
import qunar.tc.qmq.producer.tx.spring.SpringTransactionProvider;

/**
 * @author zhenwei.liu
 * @since 2019-10-17
 */
public class ClientTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientTest.class);

    private static final String TEST_SUBJECT = "old.test.subject2";
    private static final String TEST_CONSUMER_GROUP = "old.consumer.group";
    private static final String META_SERVER = "http://127.0.0.1:8080/meta/address";

    private static final ExecutorService executor = Executors.newCachedThreadPool();

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
        producerProvider.setMetaServer(META_SERVER);
        producerProvider.setTransactionProvider(new SpringTransactionProvider(dataSource));
        producerProvider.init();
    }

    public MessageConsumerProvider initConsumer(final String clientId) throws Exception {
        MessageConsumerProvider consumerProvider = new MessageConsumerProvider();
        consumerProvider.setMetaServer(META_SERVER);
        consumerProvider.setAppCode("consumer_test");
        consumerProvider.setClientIdProvider(new ClientIdProvider() {
            public String get() {
                return clientId;
            }
        });
        consumerProvider.init();
        consumerProvider.online();
        return consumerProvider;
    }

    @Test
    public void testSendAndReceiveMessage() throws Exception {

        int msgNum = 100;
        final CountDownLatch consumerCountDownLatch = new CountDownLatch(msgNum);
        final CountDownLatch producerCountDownLatch = new CountDownLatch(msgNum);

        final String client1 = "test-old-client-1";
        MessageConsumerProvider consumerProvider1 = initConsumer(client1);
        consumerProvider1.addListener(TEST_SUBJECT, TEST_CONSUMER_GROUP, new MessageListener() {
            public void onMessage(Message message) {
                LOGGER.info("{} receive message {}", client1, message.getMessageId());
                consumerCountDownLatch.countDown();
            }
        }, executor);

        final String client2 = "test-old-client-2";
        MessageConsumerProvider consumerProvider2 = initConsumer(client2);
        consumerProvider2.addListener(TEST_SUBJECT, TEST_CONSUMER_GROUP, new MessageListener() {
            public void onMessage(Message message) {
                LOGGER.info("{} receive message {}", client2, message.getMessageId());
                consumerCountDownLatch.countDown();
            }
        }, executor);

        MessageSendStateListener listener = new MessageSendStateListener() {

            public void onSuccess(Message message) {
                LOGGER.info("send message successfully {}", message.getMessageId());
                producerCountDownLatch.countDown();
            }

            public void onFailed(Message message) {
                LOGGER.error("send message error {}", message.getMessageId());
                producerCountDownLatch.countDown();
            }
        };

        for (int i = 0; i < msgNum; i++) {
            Message message = producerProvider.generateMessage(TEST_SUBJECT);
            producerProvider.sendMessage(message, listener);
        }

        consumerCountDownLatch.await();
        producerCountDownLatch.await();
    }

    @Test
    public void testSendAndReceiveDelayMessage() throws Exception {

        int msgNum = 100;
        final CountDownLatch consumerCountDownLatch = new CountDownLatch(msgNum);
        final CountDownLatch producerCountDownLatch = new CountDownLatch(msgNum);

        final String client1 = "test-old-client-1";
        MessageConsumerProvider consumerProvider1 = initConsumer(client1);
        consumerProvider1.addListener(TEST_SUBJECT, TEST_CONSUMER_GROUP, new MessageListener() {
            public void onMessage(Message message) {
                LOGGER.info("{} receive message {}", client1, message.getMessageId());
                consumerCountDownLatch.countDown();
            }
        }, executor);

        final String client2 = "test-old-client-2";
        MessageConsumerProvider consumerProvider2 = initConsumer(client2);
        consumerProvider2.addListener(TEST_SUBJECT, TEST_CONSUMER_GROUP, new MessageListener() {
            public void onMessage(Message message) {
                LOGGER.info("{} receive message {}", client2, message.getMessageId());
                consumerCountDownLatch.countDown();
            }
        }, executor);

        MessageSendStateListener listener = new MessageSendStateListener() {

            public void onSuccess(Message message) {
                LOGGER.info("send message successfully {}", message.getMessageId());
                producerCountDownLatch.countDown();
            }

            public void onFailed(Message message) {
                LOGGER.error("send message error {}", message.getMessageId());
                producerCountDownLatch.countDown();
            }
        };

        for (int i = 0; i < msgNum; i++) {
            Message message = producerProvider.generateMessage(TEST_SUBJECT);
            message.setDelayTime(5, TimeUnit.SECONDS);
            producerProvider.sendMessage(message, listener);
        }

        consumerCountDownLatch.await();
        producerCountDownLatch.await();
    }
}
