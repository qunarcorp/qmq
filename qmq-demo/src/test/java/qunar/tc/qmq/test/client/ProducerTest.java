package qunar.tc.qmq.test.client;

import org.apache.tomcat.jdbc.pool.DataSource;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.MessageSendStateListener;
import qunar.tc.qmq.producer.MessageProducerProvider;
import qunar.tc.qmq.producer.tx.spring.SpringTransactionProvider;

public class ProducerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerTest.class);
    private static DataSource dataSource;

    @BeforeClass
    public static void init() throws Exception {
        dataSource = new DataSource();
        dataSource.setUrl("jdbc:mysql://127.0.0.1:3306/qmq_produce");
        dataSource.setUsername("root");
        dataSource.setPassword("root");
    }

    @Test
    public void testBatchSendMessages() throws Exception {
        MessageProducerProvider provider = new MessageProducerProvider("producer_test", "http://127.0.0.1:8080/meta/address");
        provider.setTransactionProvider(new SpringTransactionProvider(dataSource));
        provider.init();
        for (int i = 0; i < 100; i++) {
            Message message = provider.generateMessage("new.partition.subject");
            message.setOrderKey("0");
            message.setProperty("mytag", i);
            provider.sendMessage(message, new MessageSendStateListener() {
                @Override
                public void onSuccess(Message message) {
                    LOGGER.info("send message success {}", message.getMessageId());
                }

                @Override
                public void onFailed(Message message) {
                    LOGGER.error("send message fail {}", message.getMessageId());
                }
            });
        }

        System.in.read();
    }
}
