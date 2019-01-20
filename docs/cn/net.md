[上一页](exactlyonce.md)
[回目录](../../README.md)
[下一页](delay.md)

# .NET Client

.NET Client的API和Java Client基本类似，更详细的发送消息和消费消息流程可以参考Java Client，在这里只做简单的说明。

## 代码位置
clone仓库代码后，.NET Client的代码位于clients/csharp目录下，目前没有提供编译好的dll，可以自行从代码编译，.NET Framework最低要求4.0

## 发送消息
```csharp
var producer = new MessageProducerProvider("app code", "http://<meta server address>/meta/address");
var message = producer.GenerateMessage("your subject");
message.SetProperty("key", value);

producer.Send(message, onSuccess: (m) =>{}, onFailed: (m) => { });
```

## 消费消息
```csharp
var consumer = new MessageConsumerProvider("app code", "http://<meta server address>/meta/address");
var subscriber = consumer.Subscribe("your subject", "your consumer group");
subscriber.Received += (m) => { 
    //process your message
};
subscriber.Start();
```

[上一页](exactlyonce.md)
[回目录](../../README.md)
[下一页](delay.md)
