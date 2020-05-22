[上一页](monitor.md)
[回目录](../../README.md)
[下一页](producer.md)

# Trace

消息队列服务的参与方众多，生产者发出消息后，可能存在很多的consumer订阅。如果某消息驱动的业务出现问题，定位起来将非常麻烦，如果在定位问题时能有全链路跟踪埋点，将会起到事半功倍的效果，QMQ通过接入[OpenTracing](https://opentracing.io/)规范，提供了完善的trace机制。

## 接入自己的trace系统

## QMQ中的trace埋点

### Qmq.Produce.Send
生产者发消息sendMessage方法调用时会添加该埋点，该埋点携带了subject, messageId, appCode等信息，可以用于定位sendMessage方法是否调用的问题

### Qmq.QueueSender.Send
消息真正发送给server时候会添加该埋点，该埋点可以看出消息是否发送成功

### Qmq.Consume.Process
消费者处理消息逻辑会添加该埋点，该埋点可以表示消费者处理消息吞吐量及其耗时

### Qmq.Consume.Ack
消费者消费完成消息后发送ack回server时候会添加该埋点

[上一页](monitor.md)
[回目录](../../README.md)
[下一页](producer.md)
