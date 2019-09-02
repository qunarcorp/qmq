[上一页](quickstart.md)
[回目录](../../README.md)
[下一页](design.md)

# 安装


目前根据反馈，对QMQ的安装有几个疑问，下面做一下解答：

## 安装失败情况
* 未按照安装文档进行，没有手动执行AddBroker命令注册
* 执行了AddBroker命令，但是提供了错误的参数
    
    * 错误的hostname 首先请在broker机器上执行hostname命令，找到你的机器名，如果使用该hostname仍然失败，则可以观察metaserver的日志，应该会出现 broker request BROKER_ACQUIRE_META:hostname/port 这样的日志，则这里的hostname即程序里获取你的真实的hostname，如果和hostname命令获取的不同，可能是/etc/hosts文件里进行配置。这个时候要么调整/etc/hosts文件，要么将AddBroker里的hostname调整为metaserver日志里的hostname
    * 端口号错误 AddBroker命令里的servePort对应配置里的broker.port，syncPort对应配置文件里的sync.port
    * brokerGroup错误 brokerGroup是每一组的名字，一主一从为一组，不同组的名称不能重复，delay与broker的brokerGroup也不能重复
    * ip地址错误 如果metaserver和broker部署在同一台机器上，不要使用127.0.0.1 这样的ip地址
    * 配置错了metaserver的地址，外部配置里使用的metaserver的地址都是指metaserver管理端口地址，管理端口默认为8080

## 手工注册过程麻烦
很多人反应QMQ初次使用时手工注册太麻烦，为什么不能自动进行。这里做一下说明：

qmq的生产推荐配置是一主一从成为一个group，每个group需要配置一个group name(全局唯一)，所以运维时我们需要提供：group name和主从关系。当然这些也可以放在配置文件里然后server启动后自动向metaserver注册，但是我们生产上一般不会只启动一台server，如果要启动多台server的时候每台server的配置文件里都要提供这些信息，导致每台server的配置文件都不相同。不同server配置不同并不利于自动化部署，qmq的手动注册过程相当于将这些不同的配置外置，这样所有server的配置文件都是一样的了。这主要是从运维qmq集群来考虑的。

## 下载
我们推荐你直接下载编译好的文件来运行应用。在github上可以[下载](https://github.com/qunarcorp/qmq/releases)

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
请分别在metaserver-env.sh, broker-env.sh, delay-env.sh, watchdog-env.sh, backup-env.sh里的JAVA_OPTS里配置JVM相关参数，GC日志相关参数已经配置。

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
# 每行一个token，等号左边是token，在命令中使用；等号右边是描述，只起提示作用，无实际用途
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
# 可选，动态生效，slave向master同步请求的超时时间
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
# 可选，动态生效，slave与master同步时，如果master没数据的时候，master会hold请求的最长时间，该时间不能比slave.sync.timeout长，一般不用修改
message.sync.timeout.ms=10
```
## 启动
在启动broker之前，请先将其在metaserver里注册，broker启动时候需要从metaserver获取元数据信息。运行bin目录的tools.sh(windows平台使用tools.cmd)，执行以下命令:

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
* brokerGroup 这一组的名字，每一组分为一主一从(默认可以不配置slave，但是在生产环境强烈建议配置slave，brokerGroup必须全局唯一，主从两个节点的brokerGroup必须相同，实时Server和延时Server的brokerGroup必须不能相同)
* role 角色 0 - master, 1 - slave, 5 - delay master, 6 - delay slave
* hostname 机器的主机名，注意必须是真实有效的主机名，可以使用hostname命令查看主机名
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
> broker.cmd
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
# 可选，slave web端口，用于backup查询
slave.server.http.port=8080
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
* brokerGroup 这一组的名字，每一组分为一主一从(默认可以不配置slave，但是在生产环境强烈建议配置slave，brokerGroup必须全局唯一，主从两个节点的brokerGroup必须相同，实时Server和延时Server的brokerGroup必须不能相同)
* role 角色 0 - master, 1 - slave, 5 - delay master, 6 - delay slave
* hostname 机器的主机名，注意必须是真实有效的主机名，可以使用hostname命令查看主机名
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

## Watchdog
Watchdog是在使用事务消息时的补偿服务，用于监控事务消息表中的未发送消息进行补偿，确保所有消息都发送成功。

### 最低配置
JDK 1.8

-Xmx2G -Xms2G

### 配置文件
*watchdog.properties*
```
# 必须 给watchdog分配唯一的应用标识
appCode=watchdog

# 必须 meta server address
meta.server.endpoint=http://<meta server host>:<port>/meta/address

# 可选，可以部署多个watchdog集群，namespace即集群名称，该名称需要与db注册时候的room参数相同
namespace=default
```

## 启动
使用bin目录的watchdog.sh(windows平台上请使用watchdog.cmd)启动
### Linux
```
$ watchdog.sh start
```
### Windows
```
> watchdog.cmd
```
## 停止
### Linux
```
watchdog.sh stop
```
### Windows
```
Ctrl + C
```

## Backup Server
负责消息内容、消费轨迹、死信消息的备份及查询功能。

### 最低配置
JDK 1.8

-Xmx2G -Xms2G

### 字典配置
备份需要用到一个字典表。目前仅支持基于db的字典表。

#### 基于DB的字典
运行下载的压缩包sql目录里的init_backup.sql，初始化backup需要用到的字典表数据库。
*datasource*
````
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
````

### 数据存储配置
存储配置，目前仅支持HBase。
#### 基于HBase的存储
运行下载的压缩包sql目录里的init_backup_hbase.sql，初始化backup需要用到的hbase表。

### 【可选】 server web端口配置
消息内容的查询是在slave上完成，所以如果slave web端口如果不是默认值（slave配置项slave.server.http.port），需要在backup端配置一下。  
*broker-http-port-map.properties*
```
<host>[/<serverport>]=<httpport>
<host>[/<serverport>]=<httpport>
# multi-lines
```

HBase配置文件 *hbase.properties*
```
hbase.zookeeper.quorum=
hbase.zookeeper.znode.parent=
# Also Other Settings About HBase 
```

### 配置文件
*backup.properties*  
```
# 必填 meta server address
meta.server.endpoint=http://<host>[:<port>]/meta/address
# 必填 从metaserver上获取groupname->slave的地址
acquire.server.meta.url=http://<host>[:<port>]/slave/meta
# 必填 数据存放目录
store.root=/data
# 必填 backup会用到rocksdb作为中间层临时存储
rocks.db.path=/data/rocksdb
# 可选
rocks.db.ttl=7200
# 可选 动态生效
message.backup.batch.size=10
# 可选 动态生效
message.backup.max.retry.num=5
# 可选 动态生效
record.backup.batch.size=10
# 可选 动态生效
message.backup.max.retry.num=5
# 可选 
dead.message.backup.thread.size=1
# 可选 
dead.record.backup.thread.size=1
# 可选 动态生效
enable.record=false
# 可选 broker服务端口
broker.port=20881
# 可选 存储类型，目前仅支持hbase
store.type=hbase
# 可选 web端口
backup.server.http.port=8080
```
## 启动
在启动backup server之前，请先将其在metaserver里注册，backup启动时候需要从metaserver获取元数据信息。

运行bin目录的tools.sh(windows平台使用tools.cmd)，执行以下命令:

```
# 注册实时backup server 节点  
$ tools.sh AddBroker --metaserver=<metaserver address> --token=<token> --brokerGroup=<groupName> --role=4 --hostname=<hostname> --ip=<ip> --servePort=<server port>  
```

```
# 注册延迟backup server节点(暂不支持)
```

* metaserver address指的是ip:port,port默认是8080
* token即metaserver的配置valid-api-tokens.properties里任何一项
* brokerGroup 这一组的名字，每一组分为一主一从(默认可以不配置slave，但是在生产环境强烈建议配置slave，brokerGroup必须全局唯一，主从两个节点的brokerGroup必须相同，实时Server和延时Server的brokerGroup必须不能相同)
* role 角色 0 - master, 1 - slave, 4 - backup, 5 - delay master, 6 - delay slave, 7 - delay backup(暂不支持)
* hostname 机器的主机名，注意必须是真实有效的主机名，可以使用hostname命令查看主机名
* ip 机器的ip地址
* servePort 接收消息的端口

## 启动
使用bin目录的backup.sh(windows平台上请使用backup.cmd)启动
### Linux
```
$ backup.sh start
```
### Windows
```
> backup.cmd
```
## 停止
### Linux
```
backup.sh stop
```
### Windows
```
Ctrl + C
```

## 从源码安装
我们建议大家直接下载我们编译好的包进行安装，但是你也可以进入代码目录运行下面的命令:
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

注意，运行的时候请进入target目录下编译输出里的bin下运行，而不是在源代码目录的qmq-dist/bin下运行


[上一页](quickstart.md)
[回目录](../../README.md)
[下一页](design.md)
