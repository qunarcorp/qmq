[上一页](tag.md)
[回目录](../../README.md)
[下一页](debug.md)
# 消息备份  

backup server主要工作是消息索引（包括死信消息）、消费轨迹的备份，以及消息的查询。

## 备份流程

### 消息索引
1. backup仅仅从slave同步消息索引并落盘index log，每个索引包括：subject, sequence, message id, timestamp
2. 回放本地磁盘上的index log,解析写入底层存储
    a. 如果是retry消息，按照retry消息写入
    b. 如果是dead消息，按照dead消息写入，并且需要写入一条特殊的消息记录

### 消费轨迹
1. backup从slave同步 action log
2. 同步实时解析action log，解析出具体action
3. 将解析所得的action，写入底层存储

### 存储
目前仅支持HBase作为底层存储。根据配置项`store.type`选择底层存储。
#### HBase
* message 存储消息meta信息
* deadMessage 存储死信消息
* record 存储消息轨迹

## 查询API说明
### 消息列表
**URL** /api/message?backupQuery=$query
```
# query 须为json
{
    "subject": $subject, // string 必须
    "messageId": $msgId // string 可选
    "msgCreateTimeBegin": $begin // date 可选 
    "msgCreateTimeEnd": $end    // date 可选 如无begin和end两参数，默认是查询最近30天内的消息
    "isDelay": false,    // boolean 必须 实时 or延迟
    "len": 10    // int 必须 查询条数
    "start": $start   // string 可选
}
```
**METHOD** GET  

**响应**
```
{
    "list":[
        {
            "brokerGroup": $brokerGroup,  // string 组
            "sequence": $sequence,  // long 消息seq
            "messageId": $msgId,    // string 消息ID
            "subject": $subject,    // string 主题
            "createTime": $createTime   // long 创建时间
        },{
        }
    ],
    "next": $next   // string 下一页的start
}
```
**示例**  

```
# request
GET http://local.qunar.com:8080/api/message?backupQuery={"subject":"qmq.test","isDelay":false,"len":3}
Content-Type: application/json 

# response
{
  "list": [
    {
      "subejct": "new.qmq.test",
      "messageId": "190603.154239.100.80.128.160.3316.0",
      "sequence": "3",
      "createTime": "1559547759633",
      "brokerGroup": "dev"
    },
    {
      "subejct": "new.qmq.test",
      "messageId": "190603.154134.100.80.128.160.6908.0",
      "sequence": "2",
      "createTime": "1559547695046",
      "brokerGroup": "dev"
    },
    {
      "subejct": "new.qmq.test",
      "messageId": "190603.153932.100.80.128.160.4320.0",
      "sequence": "1",
      "createTime": "1559547572860",
      "brokerGroup": "dev"
    }
  ],
  "next": "0000018093968461903b53890441300189aaad74c6abd78200"
}

```

### 死信消息列表
**URL** /api/message/dead?backupQuery=$query    query参数同消息列表参数  

**METHOD** GET  

**响应**  

```
{
    "list":[
        {
            "brokerGroup": $brokerGroup,  // string 组
            "sequence": $sequence,  // long 消息seq
            "messageId": $msgId,    // string 消息ID
            "subject": $subject,    // string 主题
            "createTime": $createTime   // long 创建时间
        },{
        }
    ],
    "next": $next   // string 下一页的start
}
```
### 消息内容
**URL** /api/message/detail?backupQuery=$query
```
# query 须为json
{
    "subject": $subject, // string 必须
    "brokerGroup": $brokerGroup,    //string 组
    "sequence": $sequence   // long 消息seq
}
```
**METHOD** GET  

**响应**
```
{
    "subject": $subject,
    "messageId": @msgId,
    "brokerGroup": $group,
    "sequence": $seq,
    "tags": [],
    "attrs": {},    // 消息属性，业务属性均在这里
    ...
}
```
**示例**
```
# request
GET http://local.qunar.com:8082/api/message/detail?backupQuery={"subject":"new.qmq.test","brokerGroup":"dev","sequence":0}
Content-Type: application/json

# response
{
  "sequence": "0",
  "brokerGroup": "dev",
  "messageId": "190603.153809.100.80.128.160.12812.0",
  "subject": "new.qmq.test",
  "tags": [],
  "storeAtFailed": false,
  "durable": true,
  "attrs": {
    "qmq_expireTime": "1559548389447",
    "qmq_appCode": "producer_test",
    "qmq_createTime": "1559547489447"
  }
}
```

### 消费轨迹
**URL** /api/message/records?recordQuery=$query
```
# query须为json格式
{
    "subject": $subject,    // string
    "messageId": $msgId,    // string
    "brokerGroup": $group,  // string 组
    "sequence": $sequence,  // long 消息seq
    "recordCode": $code     // byte 消费记录类型 0-正常消费记录，1-重试消费记录，2-死信消费记录
}
```
**METHOD** GET  

**响应**
```
{
    "records":[
        {
            "consumerGroup": $broker,
            "action": $action,  // 0-拉取，1-ack
            "type": $type,  // 0-正常消费记录，1-重试消费记录，2-死信消费记录
            "timestamp": $time,   // 消费时间
            "consumerId": $consumerId,  // 消费方ID
            "sequence": $seq
        },
        {}
    ]
}
```
**示例**
```
# 正常消息消费记录
# request 
GET http://local.qunar.com:8082/api/message/records?recordQuery={"subject":"new.qmq.test","sequence":0,"brokerGroup":"dev","recordCode":0}
Content-Type: application/json

# response
{
    "records":[
        {
            "consumerGroup":"group1",
            "action":0,
            "type":0,
            "timestamp":"1559547824956",
            "consumerId":"QPB-dengxuf@@c4ebcc16ed419345e01fed0154309621",
            "sequence":"0"
        },
        {
            "consumerGroup":"group1",
            "action":1,
            "type":0,
            "timestamp":"1559547825252",
            "consumerId":"QPB-dengxuf@@c4ebcc16ed419345e01fed0154309621",
            "sequence":"0"
        }
    ]
}

# 重试消费记录
GET http://local.qunar.com:8082/api/message/records?recordQuery={"subject":"new.qmq.test","messageId":"190603.153809.100.80.128.160.12812.0","recordCode":1}
Content-Type: application/json

# 死信消费记录
GET http://local.qunar.com:8082/api/message/records?recordQuery={"subject":"new.qmq.test","messageId":"190603.153809.100.80.128.160.12812.0","recordCode":2}
Content-Type: application/json

```

[上一页](tag.md)
[回目录](../../README.md)
[下一页](debug.md)