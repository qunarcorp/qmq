[上一页](trace.md)
[回目录](../../README.md)
[下一页](consumer.md)

# 发送消息(producer)

## 与Spring集成

### 使用xml配置方式

```xml
<?xml version="1.0" encoding="UTF-8"?>
<beans xmlns="http://www.springframework.org/schema/beans"
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xsi:schemaLocation="http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd">

    <bean class="qunar.tc.qmq.producer.MessageProducerProvider">
        <property name="appCode" value="your app" />
        <property name="metaServer" value="http://<meta server address>/meta/address" />
    </bean>
</beans>
```

```java
import qunar.tc.qmq.MessageProducer;

@Service
public class OrderService {
    
    @Resource
    private MessageProducer producer;

    public void placeOrder(Order order){
        //bussiness work

        Message message = producer.generateMessage("order.changed");
        message.setProperty("orderNo", order.getOrderNo());
        producer.sendMessage(message);
    }
}
```

### 使用注解方式

```java
public class Configuration{

    @Bean
    public MessageProducer producer(){
        MessageProducerProvider producer = new MessageProducerProvider();
        producer.setAppCode("your app");
        producer.setMetaServer("http://<meta server address>/meta/address");
        return producer;
    }
}
```

## 直接使用API发送消息
```java
MessageProducerProvider producer = new MessageProducerProvider();
producer.setAppCode("your app");
producer.setMetaServer("http://<meta server address>/meta/address");
producer.init();

//每次发消息之前请使用generateMessage生成一个Message对象，然后填充数据
Message message = producer.generateMessage("your subject");
//QMQ提供的Message是key/value的形式
message.setProperty("key", "value");

//发送消息
producer.sendMessage(message);
```

```java
//sendMessage本身是纯异步的，方法调用完毕并不表示消息就发送出去，可以使用下面的方式判断消息发送状态
producer.sendMessage(message, new MessageSendStateListener() {
    @Override
    public void onSuccess(Message message) {
        //send success
    }

    @Override
    public void onFailed(Message message) {
        //send failed
    }
});
```

另外，MessageProducerProvider提供了几个设置，可以用来调整异步发送的一些参数，默认情况下这些参数可以不设置。

```java
//发送线程数，默认是3
producer.setSendThreads(2);

//默认每次发送时最大批量大小，默认30
producer.setSendBatch(30);

//异步发送队列大小
producer.setMaxQueueSize(10000);

//如果消息发送失败，重试次数，默认10
producer.setSendTryCount(10);
```

* 注意
```
QMQ的Message.setProperty(key, value)如果value是字符串，则value的大小默认不能超过32K，如果你需要传输超大的字符串，请务必使用message.setLargeString(key, value)，这样你甚至可以传输十几兆的内容了，但是消费消息的时候也需要使用message.getLargeString(key)。
```

[上一页](trace.md)
[回目录](../../README.md)
[下一页](consumer.md)
