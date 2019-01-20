[上一页](producer.md)
[回目录](../../README.md)
[下一页](transaction.md)

# 在junit测试中发送消息

MessageProducerProvider.sendMessage方法是异步方法，该方法执行完毕并不表示消息发送成功，真正发送消息是在后台线程里进行的，所以如果后台线程还未发送消息或正在发送消息时，主线程退出了，则消息就会没有发送，这在长时间运行的应用(比如web application)中一般没有什么问题，但是在短时间运行的应用中则需要关注。比如在单元测试中，@Test方法在执行完毕后主线程就立即退出了，这就往往导致在@Test中发送消息总是未发送。那么我们可以利用sendMessage的MessageSendStateListener回调将异步转为同步。

```java
import org.junit.Before;
import org.junit.Test;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.MessageSendStateListener;
import java.util.concurrent.CountDownLatch;
import qunar.tc.qmq.producer.MessageProducerProvider;

public class Test{
    private MessageProducerProvider producer;

    @Before
    public void init(){
        producer = new MessageProducerProvider();
        producer.init();
    }

    @Test
    public void test_qmq_send_message(){
        Message message = producer.generateMessage("your subject");
        message.setProperty("key", "value");

        CountDownLatch latch = new CountDownLatch(1);
        producer.sendMessage(message, new MessageSendStateListener(){
            @Override
            public void onSuccess(Message m){
                latch.coundDown();
            }

            @Override
            public void onFailed(Message m){
                latch.countDown();
            }
        });

        try{
            latch.await();
        }catch(Exception e){}
    }
}
```

[上一页](producer.md)
[回目录](../../README.md)
[下一页](transaction.md)