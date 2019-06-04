# QMQ

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.qunar.qmq/qmq/badge.svg)](http://search.maven.org/#search%7Cga%7C1%7Ccom.qunar.qmq)
[![GitHub release](https://img.shields.io/github/release/qunarcorp/qmq.svg)](https://github.com/qunarcorp/qmq/releases)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

QMQ是去哪儿网内部广泛使用的消息中间件，自2012年诞生以来在去哪儿网所有业务场景中广泛的应用，包括跟交易息息相关的订单场景；
也包括报价搜索等高吞吐量场景。目前在公司内部日常消息qps在60W左右，生产上承载将近4W+消息topic，消息的端到端延迟可以控制在10ms以内。

主要提供以下特性：
* 异步实时消息
* 延迟/定时消息(支持任意秒级)
* 广播消息(每个Consumer都收到相同消息，比如本地cache更新)
* 基于Tag的服务端过滤
* Consumer端幂等处理支持
* Consumer端filter
* 消费端支持按条ack消息
* 死信消息
* 结合Spring annotation使用的简单API
* 提供丰富的监控指标
* 接入OpenTracing
* 事务消息
* Consumer的处理能力也可以方便扩容缩容
* Server可以随心所欲扩容缩容
* Java Client, .NET Client
* 消息投递轨迹(即将开源)
* 历史消息的自动备份(即将开源)
* 有序消息(即将开源)

# JDK最低版本要求
* Client: 1.7及其以上版本
* Server: 1.8及其以上版本

# Maven
qmq的客户端已经发布到maven中央仓库，可以通过下面的方式获取
```xml
<dependency>
    <groupId>com.qunar.qmq</groupId>
    <artifactId>qmq</artifactId>
    <version>1.1.3</version>
</dependency>
```

# 快速开始
你可以通过[设计背景](docs/cn/design.md)了解设计QMQ的初衷和它与其他消息队列的不同。
阅读[架构概览](docs/cn/arch.md)了解QMQ的存储模型

## 文档
* [快速入门](docs/cn/quickstart.md)
* [安装](docs/cn/install.md)
* [设计背景](docs/cn/design.md)
* [架构概览](docs/cn/arch.md)
* [代码模块介绍](docs/cn/code.md)
* [高可用](docs/cn/ha.md)
* [监控](docs/cn/monitor.md)
* [Trace](docs/cn/trace.md)
* [发送消息](docs/cn/producer.md)
* [在Junit Test中如何发送消息](docs/cn/unittest.md)
* [事务消息](docs/cn/transaction.md)
* [消费消息](docs/cn/consumer.md)
* [幂等Exactly once消费](docs/cn/exactlyonce.md)
* [.NET客户端](docs/cn/net.md)
* [延时/定时消息](docs/cn/delay.md)
* [服务端tag过滤](docs/cn/tag.md)
* [消息备份](docs/cn/backup.md)
* [在IDE里运行代码](docs/cn/debug.md)
* [运维](docs/cn/operations.md)
* [支持其他DB](docs/cn/db.md)
* [开源协议](docs/cn/opensource.md)
* [技术支持](docs/cn/support.md)
* [分享](docs/cn/share.md)
* [FAQ](docs/cn/faq.md)

# 技术支持

### 欢迎关注QMQ官方公众号
![公众号](docs/images/wx.jpg)

### QQ群
![QQ](docs/images/support1.png)

# 开源协议
[Apache 2 license](https://github.com/ctripcorp/apollo/blob/master/LICENSE)

# 用户(已经在生产使用)

欢迎在[这里](https://github.com/qunarcorp/qmq/issues/19)，以方便我们提供更好的技术支持

![去哪儿](docs/images/logo/qunar.png)
![携程](docs/images/logo/ctrip.png)
![IYMedia](docs/images/logo/iymedia.png)
![便利蜂](docs/images/logo/bianlifeng.png)
![金汇金融](docs/images/logo/jinhui365.png)
