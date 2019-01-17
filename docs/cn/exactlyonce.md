[上一页](consumer.md)
[回目录](../../README.md)
[下一页](net.md)

# 幂等Exactly once消费
一般消息分为At Most Once, At Least Once和Exactly once。而最后一种往往是我们最期望的模型，但是实现起来并不容易。因为网络和应用的依赖的复杂性，Exactly once基本上是不可行的，但是我们可以通过实现幂等处理达到Exactly once的效果，虽然在消息投递上还是存在重复的消息，但是对于最终的结果来说却和Exactly once达到的一样。

## 什么时候会有重复消息
* 发送消息时因为网络抖动，导致发送超时，但是实际上Server已经成功收到该消息，只是Server的Response回到producer的时候超时了，这个时候producer端为了确保消息不丢失往往回重试，这就有可能导致相同的消息发送多次
* consumer在收到消息后进行业务处理，业务处理过程中需要调用外部依赖，比如调用另外一个HTTP接口，或者写入数据库，那么这些也是通过网络进行的，那也同样存在实际成功，但是结果超时的情况，因为最终消费失败，为了确保消息的可靠性，一般针对这种情况也会进行重发消息
* consumer收到消息后处理成功，这个时候需要ack消息，但是ack消息因为网络超时等原因丢失，导致消费成功的消息没有被ack，也会导致消息重复消费

## QMQ怎么做
上面我们分析了消息重复的可能性以及Exactly once的难点。但是虽然困难，作为MQ本身来讲，还是可以提供一些措施简化幂等的处理，QMQ默认提供了基于DB的幂等处理器：

1. 首先使用下面的SQL创建表
```sql
--表名可以自定义
CREATE TABLE `deduplicate_tbl` (
  `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键',
  `k` VARCHAR(100) NOT NULL COMMENT '去重key(长度请根据自己的幂等检查函数调整)',
  `update_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_k` (`k`)
) ENGINE=INNODB DEFAULT CHARSET=utf8mb4 COMMENT '幂等去重';
```
2. 配置数据源(上表所在的db)及幂等处理器
```xml
<bean id="dataSource" class="org.apache.tomcat.jdbc.pool.DataSource" destroy-method="close">
    <property name="driverClassName" value="${jdbc.driverClassName}"/>
    <property name="url" value="${jdbc.url}"/>
    <property name="username" value="${jdbc.username}"/>
    <property name="password" value="${jdbc.password}"/>
    <property name="maxActive" value="50"/>
    <property name="maxIdle" value="5"/>
    <property name="maxWait" value="3000"/>
    <property name="validationQuery" value="select 1"/>
    <property name="testOnBorrow" value="true"/>
</bean>
<bean name="jdbcIdempotentChecker" class="qunar.tc.qmq.consumer.idempotent.JdbcIdempotentChecker">
    <constructor-arg name="dataSource" ref="dataSource" />
    <constructor-arg name="tableName" value="deduplicate_tbl" />
</bean>
```
3. 消费消息
```java
@QmqConsumer(subject = "your subject", consumerGroup = "your consumer group", idempotentChecker = "jdbcIdempotentChecker")
public void onMessage(Message message) {

}
```

通过这种配置，我们基本上就具备去重的能力，但是上面的方式也并不是完全可靠的。这种去重表的原理是收到消息后在去重表里看看是否已经消费，如果已经消费了则忽略该消息，如果未消费则插入一条记录，如果消费失败的时候我们会删除该记录。但是存在一定的概率，比如消费失败了，但是去重表的记录也没有删除掉，这种情况就会导致消息最终没有消费。

## 事务幂等处理
但是在一定的条件下我们也可以实现“真正”的Exactly once消费，如果消费消息的逻辑完全就是数据库处理，并且该数据库支持事务，那么我们就可以借助事务的原子性来实现Exactly once。

1. 将上一段提到的表建在和业务同一个db中

2. 配置

```xml
<!-- 去重表和业务共享相同的db和数据源 -->
<bean id="dataSource" class="org.apache.tomcat.jdbc.pool.DataSource" destroy-method="close">
    <property name="driverClassName" value="${jdbc.driverClassName}"/>
    <property name="url" value="${jdbc.url}"/>
    <property name="username" value="${jdbc.username}"/>
    <property name="password" value="${jdbc.password}"/>
    <property name="maxActive" value="50"/>
    <property name="maxIdle" value="5"/>
    <property name="maxWait" value="3000"/>
    <property name="validationQuery" value="select 1"/>
    <property name="testOnBorrow" value="true"/>
</bean>

<bean id="transactionManager" class="org.springframework.jdbc.datasource.DataSourceTransactionManager">
    <constructor-arg name="dataSource" ref="dataSource" />
</bean>

<bean name="jdbcIdempotentChecker" class="qunar.tc.qmq.consumer.idempotent.TransactionalJdbcIdempotentChecker">
    <constructor-arg name="transactionManager" ref="transactionManager" />
    <constructor-arg name="tableName" value="deduplicate_tbl" />
</bean>
```

3. 消费消息
```java
@QmqConsumer(subject = "your subject", consumerGroup = "your consumer group", idempotentChecker = "jdbcIdempotentChecker")
public void onMessage(Message message) {
    //业务处理在事务里进行，如果有任何异常，事务将会回滚
}
```

## 自定义幂等处理
QMQ默认提供了基于db的幂等处理器，但是使用方也可以很容易扩展自定义的幂等处理器，比如使用redis等，只需要从qunar.tc.qmq.consumer.idempotent.AbstractIdempotentChecker派生即可


[上一页](consumer.md)
[回目录](../../README.md)
[下一页](net.md)
