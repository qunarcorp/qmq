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


[上一页](debug.md)
[回目录](../../README.md)
[下一页](db.md)