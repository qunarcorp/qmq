package qunar.tc.qmq.producer;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.MessageSendStateListener;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ProducerTest {
    private static final Logger LOG = LoggerFactory.getLogger(ProducerTest.class);

    public static void main(String[] args) throws Exception {
        final MessageProducerProvider provider = new MessageProducerProvider("producer_test", "http://127.0.0.1:8080/meta/address");

        final BatchFileAppender appender = new BatchFileAppender(new File("producer.txt"), 10_000);

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
                                LOG.info("message send success id:{}", message.getMessageId());
                                appender.write(message.getMessageId());
                            }

                            @Override
                            public void onFailed(Message message) {
                                LOG.error("failed");
                            }
                        });

                        TimeUnit.SECONDS.sleep(1);
                    } catch (Exception e) {
                        LOG.error("failed send", e);
                    }
                }
            }
        }).start();

        System.in.read();
        running.set(false);
        provider.destroy();
        TimeUnit.SECONDS.sleep(5);
        appender.close();
    }
}
