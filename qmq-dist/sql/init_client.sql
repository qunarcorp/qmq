CREATE DATABASE `qmq_produce`;
CREATE TABLE `qmq_msg_queue` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `content` longtext NOT NULL,
  `status` smallint(6) NOT NULL DEFAULT '0' COMMENT '消息状态',
  `error` int unsigned NOT NULL DEFAULT '0' COMMENT '错误次数',
  `create_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='记录业务系统消息';