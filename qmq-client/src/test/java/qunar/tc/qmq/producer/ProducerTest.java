package qunar.tc.qmq.producer;

import com.google.common.base.Strings;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.MessageSendStateListener;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ProducerTest {

    private static final Logger logger = LoggerFactory.getLogger(ProducerTest.class);

    @Test
    public void testSendMessage() throws Exception {
        MessageProducerProvider provider = new MessageProducerProvider("producer_test", "http://127.0.0.1:8080/meta/address");
        provider.init();
        final String data = Strings.repeat("1a", 100);
        final AtomicBoolean running = new AtomicBoolean(true);
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (running.get()) {
                    try {
                        final Message message = provider.generateMessage("new.qmq.test");
                        for (int i = 0; i < 20; i++) {
                            message.setProperty("index" + i, data);
                        }
                        provider.sendMessage(message, new MessageSendStateListener() {
                            @Override
                            public void onSuccess(Message message) {
                                logger.info("message send success id:{}", message.getMessageId());
                            }

                            @Override
                            public void onFailed(Message message) {
                                logger.error("failed");
                            }
                        });

                    } catch (Exception e) {
                        logger.error("failed send", e);
                    } finally {
                        try {
                            TimeUnit.SECONDS.sleep(2);
                        } catch (InterruptedException e) {
                            // ignore
                        }
                    }
                }
            }
        }).start();

        System.in.read();
        running.set(false);
        provider.destroy();
        TimeUnit.SECONDS.sleep(5);
    }
}
