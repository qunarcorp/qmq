# QMQ

QMQ是去哪儿网内部广泛使用的消息中间件，自2012年诞生以来在去哪儿网所有业务场景中广泛的应用，包括跟交易息息相关的订单场景；
也包括报价搜索等高吞吐量场景。目前在公司内部日常消息qps在60W左右，生产上承载将近4W+消息topic，消息的端到端延迟可以控制在10ms以内。

主要提供以下特性：
* 异步实时消息
* 延迟/定时消息
* 基于Tag的服务端过滤
* Consumer端幂等处理支持
* Consumer端filter
* 死信消息
* 结合Spring annotation使用的简单API
* 提供丰富的监控指标
* 接入OpenTracing
* 事务消息
* 消息投递轨迹(即将开源)
* 历史消息的自动备份(即将开源)

# Maven
qmq的客户端已经发布到maven中央仓库，可以通过下面的方式获取
```xml
<dependency>
    <groupId>com.qunar.qmq</groupId>
    <artifactId>qmq</artifactId>
    <version>1.1.0</version>
</dependency>
```

# 快速开始
你可以通过[设计背景](docs/cn/design.md)了解设计QMQ的初衷和她与其他消息队列的不同。
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
* [事务消息](docs/cn/transaction.md)
* [消费消息](docs/cn/consumer.md)
* [延时/定时消息](docs/cn/delay.md)
* [服务端tag过滤](docs/cn/tag.md)
* [开源协议](docs/cn/opensource.md)
* [技术支持](docs/cn/support.md)
* [文章分享](docs/cn/article.md)

# 技术支持

### QQ群
![QQ](docs/images/support1.png)
