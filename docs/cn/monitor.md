[上一页](ha.md)
[回目录](../../readme.md)
[下一页](trace.md)


# 监控

运维可靠的消息队列服务，需要对各个环境进行充分的监控，QMQ在各个关键节点都有埋点监控。QMQ提供插件式的监控接入，默认提供Prometheus客户端的接入，如果使用其他监控工具，也很容易定制。

## 定制监控插件

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
[回目录](../../readme.md)
[下一页](trace.md)
