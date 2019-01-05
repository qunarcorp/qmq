[上一页](quickstart.md)
[回目录](../../README.md)
[下一页](design.md)

# 安装

## 下载
我们推荐你直接下载编译好的文件来运行应用。在github上可以[下载](https://github.com/qunarcorp/qmq/releases)

## 从源码安装
进入代码目录运行下面的命令:
```
mvn clean package -am -pl qmq-dist -Pdist
```
在qmq-dist/target目录下即可得到编译输出，包含以下部分:

```
conf -- 配置文件目录
bin  -- 启动脚本目录
lib  -- jar包所在目录
sql  -- 初始化db的目录
```

## Linux配置
### 修改文件句柄
QMQ需要打开大量的文件用于持久化消息等数据，如果你的集群需要承载大量消息主题请修改该参数
```
ulimit -n 655360
```
### 虚拟内存
QMQ使用mmap的方式操作文件，如果你的集群需要承载大量消息主题请修改该参数
```
sysctl -w vm.max_map_count=262144
```

## JVM基本配置
请分别在metaserver-env.sh, broker-env.sh, delay-env.sh, watchdog-env.sh里的JAVA_OPTS里配置JVM相关参数，GC日志相关参数已经配置。

## 运行MetaServer

负责集群管理和集群发现

### 最低配置
JDK 1.8

-Xmx1G -Xms1G

在生产环境为了可用性请至少部署两台meta server，然后将其放到nginx等lb后面，将这个地址配置给client和server使用

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
# metaserver的管理工具token列表，用于控制权限。下面的tools.sh工具使用时需要该token
# 每行一个token
<token 1>=<token 1 desc>
<token 2>=<token 2 desc>
```

*client_log_switch.properties*
```
# 是否输出所有主题的详细请求信息，主要用于metaserver问题诊断
default=false

# 可以控制单个主题是否输出详细请求信息
<subject a>=true
<subject b>=false
```

## 启动
使用bin目录的metaserver.sh(windows平台上请使用metaserver.cmd)启动
### Linux
```
$ metaserver.sh start
```
### Windows
```
> metaserver.cmd
```
## 停止
### Linux
```
metaserver.sh stop
```
### Windows
```
Ctrl + C
```

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
## 启动
在启动broker之前，请先将其在metaserver里注册，broker启动时候需要从metaserver获取元数据信息。运行bin目录的tools.sh(windows平台使用tools.cmd\，执行以下命令:

```
# 注册实时server的master节点
$ tools.sh AddBroker --metaserver=<metaserver address> --token=<token> --brokerGroup=<groupName> --role=0 --hostname=<hostname> --ip=<ip> --servePort=<server port> --syncPort=<sync port>
```

```
# 注册实时server的slave节点
$ tools.sh AddBroker --metaserver=<metaserver address> --token=<token> --brokerGroup=<groupName> --role=1 --hostname=<hostname> --ip=<ip> --servePort=<server port> --syncPort=<sync port>
```

* metaserver address指的是ip:port,port默认是8080
* token即metaserver的配置valid-api-tokens.properties里任何一项
* brokerGroup 这一组的名字，每一组分为一主一从(默认可以不配置slave，但是在生产环境强烈建议配置slave，brokerGroup必须全局唯一，主从两个节点的brokerGroup相同)
* role 角色 0 - master, 1 - slave, 5 - delay master, 6 - delay slave
* hostname 机器的主机名，注意必须是真实有效的主机名。linux/mac使用hostname命令查看
* ip 机器的ip地址
* servePort 接收消息的端口
* syncPort 主从同步端口

## 启动
使用bin目录的broker.sh(windows平台上请使用broker.cmd)启动
### Linux
```
$ broker.sh start
```
### Windows
```
> metaserver.cmd
```
## 停止
### Linux
```
broker.sh stop
```
### Windows
```
Ctrl + C
```

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
# 可选，一旦设置后该参数就不允许修改，如果必须修改则需要将消息数据全部清理。该参数用于控制多久生成一个刻度的schedule log（默认一个小时一个刻度），因为QMQ会预先加载一个刻度的消息索引，每条索引32 bytes，如果延时消息量极大，则默认一个小时一个刻度过大。举例：假设延时消息每秒10000 qps，则1个小时的索引大小为: 10000 * 60 * 60 * 32 / 1024 / 1024 = 1098MB。如果比qps比这个数据更高将占用更高的内存，那么如果你的延时消息的最大延时时间不是一两年之久，则可以将这个刻度调小，比如10分钟一个刻度，则10000 qps的内存占用为： 10000 * 60 * 10 * 32 / 1024 / 1024 = 5MB，这个内存占用就小多了
segment.scale.minute=60
```

## 启动
在启动delay之前，请先将其在metaserver里注册，delay启动时候需要从metaserver获取元数据信息。

运行bin目录的tools.sh(windows平台使用tools.cmd)，执行以下命令:

```
# 注册delay server的master节点
$ tools.sh AddBroker --metaserver=<metaserver address> --token=<token> --brokerGroup=<groupName> --role=5 --hostname=<hostname> --ip=<ip> --servePort=<server port> --syncPort=<sync port>
```

```
# 注册delay server的slave节点
$ tools.sh AddBroker --metaserver=<metaserver address> --token=<token> --brokerGroup=<groupName> --role=6 --hostname=<hostname> --ip=<ip> --servePort=<server port> --syncPort=<sync port>
```

* metaserver address指的是ip:port,port默认是8080
* token即metaserver的配置valid-api-tokens.properties里任何一项
* brokerGroup 这一组的名字，每一组分为一主一从(默认可以不配置slave，但是在生产环境强烈建议配置slave，brokerGroup必须全局唯一，主从两个节点的brokerGroup相同)
* role 角色 0 - master, 1 - slave, 5 - delay master, 6 - delay slave
* hostname 机器的主机名，注意必须是真实有效的主机名。linux/mac使用hostname命令查看
* ip 机器的ip地址
* servePort 接收消息的端口
* syncPort 主从同步端口

## 启动
使用bin目录的delay.sh(windows平台上请使用delay.cmd)启动
### Linux
```
$ delay.sh start
```
### Windows
```
> delay.cmd
```
## 停止
### Linux
```
delay.sh stop
```
### Windows
```
Ctrl + C
```

[上一页](quickstart.md)
[回目录](../../README.md)
[下一页](design.md)
