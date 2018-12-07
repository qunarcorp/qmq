[上一页](consumer.md)
[回目录](../../README.md)
[下一页](tag.md)

# 延时/定时消息

延时/定时消息是指生产者(producer)发送消息到server后，server并不将消息立即发送给消费者(consumer)，而是在producer指定的时间之后送达。比如在电商交易中，经常有这样的场景：下单后如果半个小时内没有支付，自动将订单取消。那么如果不使用延时/定时消息，则一般的做法是使用定时任务定期扫描订单状态表，如果半个小时后订单状态还未支付，则将订单取消。而使用延时/定时消息实现起来则比较优雅：用户下单后，发送一个延时消息，指定半个小时后消息送达，那么消费者在半个小时后收到消息就查询消息状态，如果这个时候订单是未支付状态，则取消订单。

延时/定时消息的消费与实时消息一致，请参照[消费消息](consumer.md)

注意： 延时/定时消息使用的时间都是指delay server服务器的时间，所以请确保delay server的服务器时间偏差不要太大。另外，延时/定时消息的精度在1秒左右，在业务设计时应该考虑到这一点，比如不要期望能达到延时100ms的效果。

## 发送延时消息

延时消息是指消息在当前时间之后一段时间后发送

```java
Message message = producer.generateMessage("your subject");
message.setProperty("key", "value");

//指定消息延时30分钟
message.setDelayTime(30, TimeUnit.MINUTES);

//发送消息
producer.sendMessage(message);
```

## 发送定时消息

定时消息是指指定消息的发送时间。需要注意的是如果指定的发送时间小于或等于当前时间，消息是会立即发送的

```java
Message message = producer.generateMessage("your subject");
message.setProperty("key", "value");

Date deadline = DateUtils.addMinutes(new Date(), 30);
//指定发送时间
message.setDelayTime(deadline);

//发送消息
producer.sendMessage(message);
```

[上一页](consumer.md)
[回目录](../../README.md)
[下一页](tag.md)
