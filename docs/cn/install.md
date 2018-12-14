[上一页](quickstart.md)
[回目录](../../README.md)
[下一页](design.md)

# 安装

## 下载
在github上可以[下载](https://github.com/qunarcorp/qmq/releases)我们已经打包好的压缩包

## 运行MetaServer

负责集群管理和集群发现

### 最低配置
JDK 1.8

-Xmx1G -Xms1G

在生产环境为了可用性请至少部署两台，并配置一个url用于client和server找到meta server

### 初始化数据库
运行下载的压缩包sql目录里的init.sql，初始化metaserver所需要的数据库

### 配置文件
*datasource.properties*
```
# 可选，MySQL驱动类
jdbc.driverClassName=com.mysql.jdbc.Driver
# 必填，MySQL数据库连接地址
jdbc.url=jdbc:mysql://<address>:<port>/<db>?<params>
# 必填，MySQL数据库用户名
jdbc.username=
# 必填，MySQL数据库密码
jdbc.password=
# 可选，连接池最大连接数
pool.size.max=10
```

*metaserver.properties*
```
#可选，提供http服务，用于meta server的服务发现
meta.server.discover.port=8080
#可选，以tcp的方式监听，供client和server访问
meta.server.port=20880
# 可选，内部数据缓存刷新间隔
refresh.period.seconds=5
# 可选，动态生效，broker心跳超时时间
heartbeat.timeout.ms=30000
# 可选，动态生效，每个主题至少分配多少broker group
min.group.num=2
```

*valid-api-tokens.properties*
```
# http请求的白名单token列表，用于控制权限
# 每行一个token
<token 1>=<token 1 desc>
<token 2>=<token 2 desc>
```

*client_log_switch.properties*
```
# 是否输出所有主题的详细请求信息
default=false

# 可以控制单个主题是否输出详细请求信息
<subject a>=true
<subject b>=false
```

## 启动
使用bin目录的metaserver.sh(windows平台上请使用metaserver.cmd)启动

## Server

实时消息Server

### 最低配置
JDK 1.8

-Xmx2G -Xms2G

Server需要将消息写入磁盘，磁盘性能和机器空闲内存是影响其性能的关键因素

### 配置文件
*broker.properties*
```
# 必填，metaserver地址，即你第一步安装的meta server的ip地址，注意这里的地址的端口是meta.server.discover.port指定的端口，默认是8080
meta.server.endpoint=http://<metaserver address>/meta/address
# 可选，broker服务端口
broker.port=20881
# 可选，同步数据端口
sync.port=20882
# 可选，动态生效，从机同步请求超时时间
slave.sync.timeout=3000
# 必填，数据存放目录
store.root=/data
# 可选，动态生效，主是否等待从写入完成再返回写入结果
wait.slave.wrote=false
# 可选，动态生效，重试消息延迟派发时间
message.retry.delay.seconds=5
# 可选，动态生效，messagelog过期时间
messagelog.retention.hours=72
# 可选，动态生效，consumerlog过期时间
consumerlog.retention.hours=72
# 可选，动态生效，pulllog过期时间
pulllog.retention.hours
# 可选，动态生效，数据文件过期检查周期
log.retention.check.interval.seconds
# 可选，动态生效，是否删除过期数据
log.expired.delete.enable=true
# 可选，动态生效，checkpoint文件保留数量
checkpoint.retain.count=5
# 可选，动态生效，action checkpoint强制写入周期，单位为日志数量
action.checkpoint.interval=100000
# 可选，动态生效，message checkpoint强制写入周期，单位为日志数量
message.checkpoint.interval=100000
# 可选，动态生效，重试消息写入QPS限制
put_need_retry_message.limiter=50
# 可选，动态生效，从机一次最多拉取多少数据
sync.batch.size=100000
# 可选，动态生效，从机同步数据超时时间
message.sync.timeout.ms=10
```
##启动
在启动broker之前，请先将其在metaserver里注册，broker启动时候需要从metaserver获取元数据信息。

运行bin目录的tools.sh(windows平台使用tools.cmd)，执行以下命令:

```
>tools.sh AddBroker --metaserver=<metaserver address> --token=<token> --brokerGroup=<groupName> --role=0 --hostname=<hostname> --ip=<ip> --servePort=20881 --syncPort=20882
```
* metaserver address指的是ip:port,port默认是8080
* token即metaserver的配置valid-api-tokens.properties里任何一项
* brokerGroup 这一组的名字，每一组分为主从
* role 角色 0 - master, 1 - slave, 5 - delay master, 6 - delay slave
* hostname broker的主机名
* ip broker的ip地址
* servePort broker接收消息的端口
* syncPort 主从同步端口

## Delay Server

延迟/定时消息Server

### 最低配置
JDK 1.8

-Xmx2G -Xms2G

Delay Server需要将消息写入磁盘，磁盘性能和机器空闲内存是影响其性能的关键因素

### 配置文件
*delay.properties*
```
# 必填，metaserver地址，即你第一步安装的meta server的ip地址，注意这里的地址的端口是meta.server.discover.port指定的端口，默认是8080
meta.server.endpoint=http://<metaserver address>/meta/address
# 可选，broker服务端口
broker.port=20881
# 可选，同步数据端口
sync.port=20882
# 可选，动态生效，从机同步请求超时时间
slave.sync.timeout=3000
# 必填，数据存放目录
store.root=/data
# 可选，动态生效，主是否等待从写入完成再返回写入结果
wait.slave.wrote=true
# 可选，动态生效，从机一次最多拉取多少数据
sync.batch.size=100000
# 可选，动态生效，从机同步dispatch数据超时时间
dispatch.sync.timeout.ms=10
# 可选，动态生效，从机同步message数据超时时间
message.sync.timeout.ms=10
# 可选，动态生效，是否删除过期数据
log.expired.delete.enable=true
# 可选，动态生效，数据文件过期检查周期
log.retention.check.interval.seconds=60
# 可选，动态生效，dispatch文件过期时间
dispatch.log.keep.hour=72
# 可选，动态生效，messagelog过期时间
messagelog.retention.hours=72
```

##启动
在启动delay之前，请先将其在metaserver里注册，delay启动时候需要从metaserver获取元数据信息。

运行bin目录的tools.sh(windows平台使用tools.cmd)，执行以下命令:

```
>tools.sh AddBroker --metaserver=<metaserver address> --token=<token> --brokerGroup=<groupName> --role=5 --hostname=<hostname> --ip=<ip> --servePort=20881 --syncPort=20882
```
* metaserver address指的是ip:port,port默认是8080
* token即metaserver的配置valid-api-tokens.properties里任何一项
* brokerGroup 这一组的名字，每一组分为主从
* role 角色 0 - master, 1 - slave, 5 - delay master, 6 - delay slave
* hostname broker的主机名
* ip broker的ip地址
* servePort broker接收消息的端口
* syncPort 主从同步端口

[上一页](quickstart.md)
[回目录](../../README.md)
[下一页](design.md)
