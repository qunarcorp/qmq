package qunar.tc.qmq.producer;

import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.MessageProducer;
import qunar.tc.qmq.MessageSendStateListener;

import java.io.IOException;

public class Test {
    public static void main(String[] args) throws IOException {
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("spring.xml");
        context.start();

        final MessageProducer producer = context.getBean(MessageProducer.class);
        DataSourceTransactionManager transactionManager = context.getBean(DataSourceTransactionManager.class);
        TransactionTemplate template = new TransactionTemplate(transactionManager);
        template.execute(new TransactionCallback<Object>() {
            @Override
            public Object doInTransaction(TransactionStatus status) {
                Message message = producer.generateMessage("test.subject");
                message.setProperty("order", "11111");
                producer.sendMessage(message, new MessageSendStateListener() {
                    @Override
                    public void onSuccess(Message message) {
                        System.out.println("succ" + message.getMessageId());
                    }

                    @Override
                    public void onFailed(Message message) {
                        System.out.println("fail" + message.getMessageId());
                    }
                });
                return null;
            }
        });

        System.in.read();
    }
}
