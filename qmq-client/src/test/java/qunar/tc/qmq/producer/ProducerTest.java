package qunar.tc.qmq.producer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.MessageSendStateListener;

public class ProducerTest {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerTest.class);

    public static void main(String[] args) throws Exception {
        final MessageProducerProvider provider = new MessageProducerProvider();
        provider.setAppCode("producer_test");
        provider.setMetaServer("http://127.0.0.1:8080/meta/address");
        provider.init();

        int msgNum = 10;
        final CountDownLatch countDownLatch = new CountDownLatch(msgNum);
        for (int i = 0; i < msgNum; i++) {
            final Message message = provider.generateMessage("new.qmq.test");
            provider.sendMessage(message, new MessageSendStateListener() {
                @Override
                public void onSuccess(Message message) {
                    countDownLatch.countDown();
                    LOG.info("message send success id:{}", message.getMessageId());
                }

                @Override
                public void onFailed(Message message) {
                    countDownLatch.countDown();
                    LOG.info("message send failed id:{}", message.getMessageId());
                }
            });
        }

        countDownLatch.await();
        provider.destroy();
    }
}
