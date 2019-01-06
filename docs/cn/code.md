[上一页](arch.md)
[回目录](../../README.md)
[下一页](ha.md)

# 代码模块介绍

### qmq-api
暴露给用户的一些interface

### qmq-common
一些公用的类，所有其他模块都可能引用

### qmq-server-common
公用的类，只有server side应用引用，不暴露给client side

### qmq-server
实时消息服务

### qmq-delay-server
延时/定时消息服务

### qmq-store
存储

### qmq-remoting
网络相关

### qmq-client
客户端逻辑

### qmq-metrics-prometheus
提供的prometheus监控接入

### qmq-watchdog
提供消息补偿服务，从客户端消息库扫描出发送失败的消息进行补偿

### qmq-gateway
提供其他协议转换，目前提供HTTP接入

[上一页](arch.md)
[回目录](../../README.md)
[下一页](ha.md)
