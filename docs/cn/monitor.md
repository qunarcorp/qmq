[上一页](ha.md)
[回目录](../../README.md)
[下一页](trace.md)


# 监控

运维可靠的消息队列服务，需要对各个环境进行充分的监控，QMQ在各个关键节点都有埋点监控。QMQ提供插件式的监控接入，默认提供Prometheus客户端的接入([这里](https://github.com/qunarcorp/qmq/issues/33)有已经配置了Prometheus的同学回复的issue，可以参考)，如果使用其他监控工具，也很容易定制。

## 使用Prometheus
QMQ默认提供了Prometheus的接入方式，按照下面的方式操作即可:

### 对于server端(包括metaserver, broker, delay broker)
去maven仓库下载下面的jar包，将其放置在server端的lib目录中

[qmq-metrics-prometheus](http://central.maven.org/maven2/com/qunar/qmq/qmq-metrics-prometheus/1.1.1/qmq-metrics-prometheus-1.1.1.jar)

[prometheus-client](http://central.maven.org/maven2/io/prometheus/simpleclient/0.6.0/simpleclient-0.6.0.jar)

[prometheus-common](http://central.maven.org/maven2/io/prometheus/simpleclient_common/0.6.0/simpleclient_common-0.6.0.jar)

[prometheus-httpserver](http://central.maven.org/maven2/io/prometheus/simpleclient_httpserver/0.6.0/simpleclient_httpserver-0.6.0.jar)

[prometheus-graphite](http://central.maven.org/maven2/io/prometheus/simpleclient_graphite_bridge/0.6.0/simpleclient_graphite_bridge-0.6.0.jar)


默认情况下Prometheus会以http的形式在3333端口暴露http监控指标，你可以添加qmq.prometheus.properties配置修改该端口号:
```
monitor.port=3333
```

另外也提供了Graphite方式的接入，只需要在里按照如下配置即可：
```
monitor.type=graphite
graphite.host=<host>
graphite.port=<port>
```

### 客户端引入
```xml
<dependency>
    <groupId>com.qunar.qmq</groupId>
    <artifactId>qmq-metrics-prometheus</artifactId>
    <version>1.1.1</version>
</dependency>
```

如果客户端应用已经使用了prometheus，那么你可能不再希望qmq client暴露另外一个prometheus http server，则可以在qmq.prometheus.properties添加下面的配置关闭：
```
monitor.action=none
```

## 定制监控插件

QMQ使用SPI的机制提供第三方监控的接入能力，QMQ默认提供了Prometheus的接入。如果要接入一个第三方监控系统的话可以按照如下方式进行：

1. 创建一个maven工程，引入下面的依赖
```xml
<dependency>
    <groupId>com.qunar.qmq</groupId>
    <artifactId>qmq</artifactId>
    <version>1.1.0</version>
    <!--注意这里的optinal-->
    <optional>true</optional>
</dependency>
```

2. 实现QmqCounter, QmqMeter, QmqTimer, QmqMetricRegistry几个接口
* [QmqCounter](https://github.com/qunarcorp/qmq/blob/master/qmq-common/src/main/java/qunar/tc/qmq/metrics/QmqCounter.java) 计数监控，比如每分钟发送的消息条数。请参考[PrometheusQmqCounter](https://github.com/qunarcorp/qmq/blob/master/qmq-metrics-prometheus/src/main/java/qunar/tc/qmq/metrics/prometheus/PrometheusQmqCounter.java)了解如何实现。
* [QmqMeter](https://github.com/qunarcorp/qmq/blob/master/qmq-common/src/main/java/qunar/tc/qmq/metrics/QmqMeter.java) qps/tps 监控，比如发送的qps。请参考[PrometheusQmqMeter](https://github.com/qunarcorp/qmq/blob/master/qmq-metrics-prometheus/src/main/java/qunar/tc/qmq/metrics/prometheus/PrometheusQmqMeter.java)了解如何实现。
* [QmqTimer](https://github.com/qunarcorp/qmq/blob/master/qmq-common/src/main/java/qunar/tc/qmq/metrics/QmqTimer.java) 时长监控，比如发送消息耗时。请参考[PrometheusQmqTimer](https://github.com/qunarcorp/qmq/blob/master/qmq-metrics-prometheus/src/main/java/qunar/tc/qmq/metrics/prometheus/PrometheusQmqTimer.java)了解如何实现。
* [QmqMetricRegistry](https://github.com/qunarcorp/qmq/blob/master/qmq-common/src/main/java/qunar/tc/qmq/metrics/QmqMetricRegistry.java) SPI入口类。请参考[PrometheusQmqMetricRegistry](https://github.com/qunarcorp/qmq/blob/master/qmq-metrics-prometheus/src/main/java/qunar/tc/qmq/metrics/prometheus/PrometheusQmqMetricRegistry.java)了解如何实现。QmqMetricRegistry类只会初始化一次，所以一些监控的初始化工作可以在构造函数里进行。

3. 在resources下创建META-INF/services文件夹，在里面创建名为qunar.tc.qmq.metrics.QmqMetricRegistry的文本文件，文件内容即QmqMetricRegistry实现类的全名(包括包名)。比如：qunar.tc.qmq.metrics.prometheus.PrometheusQmqMetricRegistry


## Server监控指标

下面列出一些重要的监控，还有其他一些未列出监控可以从qunar.tc.qmq.monitor.Qmon.java 来查看

| 指标名称 | 指标描述 |tag|
|---------|---------|----|
|receivedMessagesEx|接收消息qps| subject|
|receivedMessagesCount| 接收消息量|subject|
|produceTime|消息发送耗时|subject|
|putMessageTime|append消息耗时|subject|
|pulledMessagesEx|拉取消息qps|subject, group|
|pulledMessagesCount|拉取消息量|subject, group|
|pullQueueTime|拉取消息时排队时间|subject, group|
|pullRequestEx|拉取请求qps|subject, group|
|pullRequestCount|拉取请求量|subject, group|
|ackRequestEx|ack请求qps|subject,group|
|ackRequestCount|ack请求量|subject,group|
|consumerAckCount|消费者发回ack的量|subject, group|
|pullProcessTime|处理拉取耗时|subject, group|
|messageSequenceLag|消息堆积数量|subject, group|
|slaveMessageLogLag|消息主从同步延迟||

## Delay Server监控

| 指标名称 | 指标描述 |tag|
|---------|---------|----|
|receivedMessagesEx|接收消息qps| subject|
|receivedMessagesCount| 接收消息量|subject|
|produceTime|消息发送耗时|subject|
|delayTime|消息实际投递时间与期望投递之间的差值|subject, group(server组)|
|scheduleDispatch|投递的消息量| - |
|appendMessageLogCostTime|消息持久化时间| subject |
|loadSegmentTimer|预加载索引耗时| - |

## Meta Server监控

| 指标名称 | 指标描述 |tag|
|---------|---------|----|
|brokerRegisterCount|server与meta之间通信次数|groupName(server集群名称), requestType(通信类型)|
|clientRegisterCount|客户端与meta之间通信次数|clientTypeCode(producer or consumer), subject|

[上一页](ha.md)
[回目录](../../README.md)
[下一页](trace.md)
