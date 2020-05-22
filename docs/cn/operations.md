[上一页](debug.md)
[回目录](../../README.md)
[下一页](db.md)

# 运维

从1.1.3.1版本开始支持

消息服务器是有状态的，当我们进行正常停机运维的时候一般期望将Server里已经收到的消息消耗完才下线维护，QMQ提供了运维工具可以进行此操作。另外，如果在发布新的服务端版本的时候，如果不想因为发布带来消息延时，也可以采用该工具先停止接收新的消息，只提供消费服务。

## init
如果是是在原有的集群里添加该功能，则需要重新执行sql/init.sql，需要添加一张数据库表

## mark read only
标记某组server只提供消息消费，不再接收消息

```
$ tools.sh MarkReadonly --metaserver=<metaserver address> --token=<token> --brokerGroup=<groupName>
```

## unmark read only
将之前标记为read only的server重新上线

```
$ tools.sh UnMarkReadonly --metaserver=<metaserver address> --token=<token> --brokerGroup=<groupName>
```

## reset offset
调整指定消费组的消费进度

```
$ tools.sh ResetOffset --metaserver=<metaserver address> --token=<token> --subject=<subject> --group=<consumer group> --action=<1或者2， LATEST=1, EARLIEST=2>
```

## 主从切换

目前开源的版本暂不支持自动主从切换机制，如果发生机器故障需要主从切换，需要人工介入，下面描述人工主从切换的步骤：

1. 将某组设置为readonly，设置readonly之后，该组将不再接受新的消息。需要注意的是并不是设置readonly后会立即就不接受新消息了，客户端需要一定的时候刷新路由，这个时间默认在1分钟左右，你可以通过观察server的receivedMessagesCount监控来确定完全没有消息进入了。使用下面的命令设置readonly：
```
tools.sh MarkReadonly --metaserver=<metaserver address> --token=<token> --brokerGroup=<broker group name>
```

2. 等待主从完全同步，一般来讲，如果打开了wait.slave.wrote，则停止接受新消息后应该主从就已经同步上了，但是你也可以通过观察master的slaveMessageLogLag监控了解

3. 停止slave， 停止master

4. 执行ReplaceBroker命令切换角色(该步骤一定要在停止应用之后执行)

```
//将原来的master里机器名，ip等修改为原来的slave
tools.sh ReplaceBroker --metaserver=<metaserver address> --token=<token> --brokerGroup=<broker group name> --role=<0, 0是master> --hostname=<原slave的机器名> --ip=<原slave的ip> --servePort=<原slave的servePort> --syncPort=<原slave的syncPort>

//将原来的slave里机器名，ip等修改为原来的slave
tools.sh ReplaceBroker --metaserver=<metaserver address> --token=<token> --brokerGroup=<broker group name> --role=<1, 1是slave> --hostname=<原master的机器名> --ip=<原master的ip> --servePort=<原master的servePort> --syncPort=<原master的syncPort>
```

5. 启动slave， 启动master

[上一页](debug.md)
[回目录](../../README.md)
[下一页](db.md)