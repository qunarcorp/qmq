package qunar.tc.qmq.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.MessageSendStateListener;

import java.util.concurrent.TimeUnit;

public class ProducerTest {
    private static final Logger LOG = LoggerFactory.getLogger(ProducerTest.class);

    public static void main(String[] args) throws InterruptedException {
        final MessageProducerProvider provider = new MessageProducerProvider();
        provider.setAppCode("producer_test");
        provider.setMetaServer("http://127.0.0.1:8080/meta/address");
        provider.init();

        try {
            final Message message = provider.generateMessage("new.qmq.test");
            provider.sendMessage(message, new MessageSendStateListener() {
                @Override
                public void onSuccess(Message message) {
                    LOG.info("message send success id:{}", message.getMessageId());
                }

                @Override
                public void onFailed(Message message) {
                    LOG.error("failed");
                }
            });
        } catch (Exception e) {
            LOG.error("failed send", e);
        }

        TimeUnit.DAYS.sleep(1);
    }
}
