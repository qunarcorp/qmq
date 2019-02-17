qmq-demo是一个包含发送消息和消费消息的完整SprintBoot demo。你可以查看[事务消息](../docs/cn/transaction.md)文档了解更多事务消息使用细节。

* 在application.properties里根据你的环境配置如下三个参数
```
# 数据库地址
spring.datasource.url=
# 数据库帐号
spring.datasource.username=
# 数据库密码
spring.datasource.password=
```

* 为了发送事务消息，请在上面db的相同实例上使用qmq-dist/sql/init_client.sql创建db

* 运行 SpringBootMain

* 访问 http://127.0.0.1:8080/order/placeOrder 就会展示下单界面，点击提交后会发送事务消息。该操作会在上面的db的orders表里持久化一条订单数据，这个时候如果因为网络或其他原因，消息没有发送到server成功的话，在qmq_produce.qmq_msg_queue表里会有一条消息记录。