[上一页](producer.md)
[回目录](../../README.md)
[下一页](delay.md)


# 消费消息(consumer)

## 与Spring结合

QMQ除了提供使用API来消费消息的方式外，还提供了跟Spring结合的基于annotation的API，我们更推荐使用这种方式。QMQ已经与SpringBoot进行了集成，如果项目使用SpringBoot则只需要引入qmq-client.jar就可以直接使用annotation的API，如果使用传统Spring的话则需要在Spring的xml里进行如下配置：
```xml
<?xml version="1.0" encoding="UTF-8"?>
<beans xmlns="http://www.springframework.org/schema/beans"
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xmlns:qmq="http://www.qunar.com/schema/qmq"
    xmlns:context="http://www.springframework.org/schema/context"
	xsi:schemaLocation="http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd
    http://www.springframework.org/schema/context http://www.springframework.org/schema/context/spring-context.xsd
	http://www.qunar.com/schema/qmq http://www.qunar.com/schema/qmq/qmq.xsd">

    <qmq:consumer appCode="your app" metaServer="http://meta server/meta/address" />

    <context:annotation-config />
    <context:component-scan base-package="qunar.tc.qmq.demo.consumer.*" />

    <!-- 供处理消息用的线程池 -->
    <bean id="qmqExecutor" class="org.springframework.scheduling.concurrent.ThreadPoolExecutorFactoryBean">
        <property name="corePoolSize" value="2" />
        <property name="maxPoolSize" value="2" />
        <property name="queueCapacity" value="1000" />
        <property name="threadNamePrefix" value="qmq-process" />
    </bean>
</beans>
```

当然，如果你的应用使用的是Spring annotation的配置方式，没有xml，那么也可以使用@EnableQmq的方式配置
```java
@Configuration
@EnableQmq(appCode="your app", metaServer="http://<meta server address>/meta/address")
public class Config{}
```

使用下面的代码就可以订阅消息了，是不是非常简单。
```java
@QmqConsumer(subject = "your subject", consumerGroup = "group", executor = "your executor bean name")
public void onMessage(Message message){
    //process your message
    String value = message.getStringProperty("key");
}
```
使用上面的方式订阅消息时，如果QmqConsumer标记的onMessage方法抛出异常，则该方法被认为是消费失败，消费失败的消息会再次消费，默认再次消费的间隔是5秒钟，这个可以进行配置。这里需要注意的是，如果有些通过重试也无法消除的异常，请将其在onMessage方法里捕获，而通过重试可以恢复的异常才抛出。

### 仅消费一次
有些消息的可靠性可能要求不高，不管是消费成功还是失败，仅仅消费一次即可，不期望重试，那么可以设置仅消费一次
```java
@QmqConsumer(subject = "your subject", consumerGroup="group", consumeMostOnce = true, executor = "your executor bean name")
public void onMessage(Message message){
    //process your message
    String value = message.getStringProperty("key");
}
```

### 广播消息
有这样的场景，我们每台机器都维护进程内内存，当数据有变更的时候，变更方会发送变更消息触发缓存更新，那么这个时候我们期望消费者每台机器都收到消息，这就是广播消息的场景了。
```java
@QmqConsumer(subject = "your subject", consumerGroup="group", isBroadcast = true, executor = "your executor bean name")
public void onMessage(Message message){
    //update local cache
}
```

### 消费端过滤器(filter)
可以将一些公共逻辑放在filter里，这样可以将filter配置在所有消费者上。比如在QMQ里内置了opentracing就是通过filter实现的，不过这个filter是内置的，不需要额外配置。
```java

@Compoent
public class LogFilter implements Filter {

    //在处理消息之前执行
    public boolean preOnMessage(Message message, Map<String, Object> filterContext){

    }

    //在处理消息之后执行
    public void postOnMessage(Message message, Throwable e, Map<String, Object> filterContext){

    }
}

@QmqConsumer(subject = "your subject", consumerGroup="group", filters = {"logFilter"}, executor = "your executor bean name")
public void onMessage(Message message){
    //update local cache
}
```

## 非Spring API
如果在非Spring环境中使用QMQ那就需要直接使用API了。QMQ提供了两种API：Listener和纯Pull。

### Listener

Listener的方式与@QmqConsumer提供的功能基本类似

```java
//推荐一个应用里只创建一个实例
MessageConsumerProvider consumer = new MessageConsumerProvider();
consumer.setAppCode("your app");
consumer.setMetaServer("http://<meta server address>/meta/address");
consumer.init();

//ThreadPoolExecutor根据实际业务场景进行配置
consumer.addListener("your subject", "group", (m) -> {
    //process message
}, new ThreadPoolExecutor(2,2,1,TimeUnit.MINUTES,new LinkedBlockingQueue<Runnable>(100)));
```

### Pull API

Pull API是最基础的API，需要考虑更多情况，如无必要，我们推荐使用annotation或者Listener的方式。

```java
//推荐一个应用里只创建一个实例
MessageConsumerProvider consumer = new MessageConsumerProvider();
consumer.setAppCode("your app");
consumer.setMetaServer("http://<meta server address>/meta/address");
consumer.init();

PullConsumer pullConsumer = consumer.getOrCreatePullConsumer("your subject", "group", false);
List<Message> messages = pullConsumer.pull(100, 1000);
for(Message message : messages){
    //process message

    //对于pull的使用方式，pull到的每一条消息都必须ack，如果处理成功ack的第二个参数为null
    message.ack(1000, null);

    //处理失败，则ack的第二个参数传入Throwable对象
    //message.ack(1000, new Exception("消费失败"));
}
```

* 注意
```
QMQ的Message.setProperty(key, value)如果value是字符串，则value的大小默认不能超过32K，如果你需要传输超大的字符串，请务必使用message.setLargeString(key, value)，这样你甚至可以传输十几兆的内容了，但是消费消息的时候也需要使用message.getLargeString(key)。
```

[上一页](producer.md)
[回目录](../../README.md)
[下一页](delay.md)
