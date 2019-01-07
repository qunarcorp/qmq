[上一页](operations.md)
[回目录](../../README.md)
[下一页](opensource.md)

# 支持其他db

QMQ在Producer端利用数据库事务来实现事务消息，为了对使用方透明，我们将该db放置在和业务db相同实例的另外一个db里，也就是如下图的方式：

![img](../images/mysql1.png)

DB A是业务使用的db，DB B是QMQ的消息DB(qmq_produce)。这样使用方根本不用感知到消息db的存在，而Producer端则使用类似下面的语句去操作消息db:
```sql
INSERT INTO qmq_produce.qmq_msg_queue(content,create_time) VALUES(?,?)
```

但是在某些db里这种操作并不一定可行，比如在SQL Server里开启了Always On特性之后，并不允许这种跨库事务。那么对于这种情况我们只能将消息的表创建在业务db中。QMQ提供了这种抽象，使用方可以自己选择，甚至可以自定义SQL语句：

```xml
    <!-- 配置数据源，业务操作和qmq共享使用 -->
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

     <bean id="sqlStatementProvider" class="qunar.tc.qmq.producer.tx.DefaultSqlStatementProvider">
        <!-- 将表名指定为 qmq_msg_queue，不使用qmq_produce.qmq_msg_queue -->
        <constructor-arg name="tableName" value="qmq_msg_queue" />
    </bean>

    <bean id="transactionProvider" class="qunar.tc.qmq.producer.tx.spring.SpringTransactionProvider">
        <constructor-arg name="bizDataSource" ref="dataSource" />
        <constructor-arg name="sqlStatementProvider" ref="sqlStatementProvider" />
    </bean>

    <bean id="messageProducer" class="qunar.tc.qmq.producer.MessageProducerProvider">
        <property name="transactionProvider" ref="transactionProvider" />
    </bean>
```

[上一页](operations.md)
[回目录](../../README.md)
[下一页](opensource.md)