[上一页](delay.md)
[回目录](../../README.md)
[下一页](debug.md)


# 服务端tag过滤

有的时候一个topic的消息里又分为不同的类别，而不同的消费方可能对不同的类别感兴趣，当然这可以通过在consumer端进行过滤实现，但同时也增加了consumer的复杂性。通过tag过滤在server端就可以过滤掉不感兴趣的类别了。

举例来说，有一个订单变更（order.changed）的消息，然后订单变更有很多不同的类别：订单创建(create), 支付(pay), 取消(cancel)等等。那么有一个应用只关注新创建订单，对其他类别不关心，而一个出票应用只会关心支付和取消的订单，对其他类别不感兴趣。这就可以使用tag过滤功能了。

```java
//发送方在发送消息时需要标记类别的tag
Message message = producer.generateMessage("order.changed");
message.setProperty("orderId", "12345");
//这是一个支付消息
message.addTag("pay");
producer.sendMessage(message);
```

消费消息时指定感兴趣的tag

```java
//不同的消费方关注不同的类别，现在这是一个出票应用，关注支付和取消
@QmqConsumer(topic = "order.changed", consumerGroup = "ticket", tags = {"pay", "cancel"}, tagType = TagType.OR, executor = "your executor")
public void onMessage(Message message){
    //只会收到pay或者cancel的消息
}
```

消费消息时跟tag相关的有两项：TagType和tags，下面对tag的匹配规则进行描述：
* 如果consumer未指定任何tag或者tagType = TagType.NO_TAG则producer发送的任何消息都会订阅，这是默认情况
* 如果consumer指定了tag，但是该消息未携带任何tag,则不匹配
* 如果consumer指定了tag，并且tagType = TagType.AND，则consumer指定的tag是该消息的子集才会匹配
* 如果consumer指定了tag, 并且tagType = TagType.OR，则consumer指定的tag与该消息的tag有交集就会匹配

[上一页](delay.md)
[回目录](../../README.md)
[下一页](debug.md)