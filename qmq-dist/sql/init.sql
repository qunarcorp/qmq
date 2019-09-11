
CREATE TABLE IF NOT EXISTS `broker_group`
(
  `id`             INT(11) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `group_name`     VARCHAR(30)      NOT NULL DEFAULT '' COMMENT 'group名字',
  `kind`           INT              NOT NULL DEFAULT '-1' COMMENT '类型',
  `master_address` VARCHAR(25)      NOT NULL DEFAULT '' COMMENT 'master地址,ip:port',
  `broker_state`   TINYINT          NOT NULL DEFAULT '-1' COMMENT 'broker master 状态',
  `tag`            VARCHAR(30)      NOT NULL DEFAULT '' COMMENT '分组的标签',
  `create_time`    TIMESTAMP        NOT NULL DEFAULT '1970-01-01 08:00:01' COMMENT '创建时间',
  `update_time`    TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_group_name` (`group_name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT = 'broker组信息';

CREATE TABLE IF NOT EXISTS `subject_info`
(
  `id`          INT(11) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `name`        VARCHAR(100)     NOT NULL DEFAULT '' COMMENT '主题名',
  `tag`         VARCHAR(30)      NOT NULL DEFAULT '' COMMENT 'tag',
  `create_time` TIMESTAMP        NOT NULL DEFAULT '1970-01-01 08:00:01' COMMENT '创建时间',
  `update_time` TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_name` (`name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COMMENT = '主题信息';

CREATE TABLE IF NOT EXISTS `subject_route`
(
  `id`                INT(11) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `subject_info`      VARCHAR(100)     NOT NULL DEFAULT '' COMMENT '主题',
  `broker_group_json` VARCHAR(300)     NOT NULL DEFAULT '' COMMENT 'broker consumerGroup name信息，json存储',
  `version`           INT              NOT NULL DEFAULT 0 COMMENT '版本信息',
  `create_time`       TIMESTAMP        NOT NULL DEFAULT '1970-01-01 08:00:01'
    COMMENT '创建时间',
  `update_time`       TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    COMMENT '修改时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_subject` (`subject_info`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COMMENT = '主题路由信息';

CREATE TABLE IF NOT EXISTS `client_offline_state`
(
  `id`             INT(11) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `client_id`      VARCHAR(100)     NOT NULL DEFAULT '' COMMENT 'client id',
  `subject_info`   VARCHAR(100)     NOT NULL DEFAULT '' COMMENT '主题',
  `consumer_group` VARCHAR(100)     NOT NULL DEFAULT '' COMMENT '消费组',
  `state`          TINYINT          NOT NULL DEFAULT 0 COMMENT '上下线状态，0上线，1下线',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_client_id_subject_group` (`client_id`, `subject_info`, `consumer_group`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COMMENT = '下线的client';

CREATE TABLE IF NOT EXISTS `broker`
(
  `id`          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `group_name`  VARCHAR(30)     NOT NULL DEFAULT '' COMMENT 'group名字',
  `hostname`    VARCHAR(50)     NOT NULL DEFAULT '' COMMENT '机器名',
  `ip`          INT UNSIGNED    NOT NULL DEFAULT '0' COMMENT '机器IP',
  `role`        INT             NOT NULL DEFAULT '-1' COMMENT '角色',
  `serve_port`  INT             NOT NULL DEFAULT '20881' COMMENT '客户端请求端口',
  `sync_port`   INT             NOT NULL DEFAULT '20882' COMMENT '从机同步端口',
  `create_time` TIMESTAMP       NOT NULL DEFAULT '2018-01-01 01:01:01' COMMENT '创建时间',
  `update_time` TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',

  PRIMARY KEY (`id`),
  UNIQUE KEY uniq_broker (`hostname`, `serve_port`),
  UNIQUE KEY uniq_group_role (`group_name`, `role`),
  KEY idx_broker_group (`group_name`)
) ENGINE InnoDB DEFAULT CHARSET = utf8mb4 COMMENT 'broker列表';

CREATE TABLE IF NOT EXISTS `leader_election` (
  `id` int(10) unsigned not null auto_increment comment '主键id',
  `name` varchar(128) not null default '' comment '名称',
  `node` varchar(128) NOT NULL default '' comment '节点',
  `last_seen_active` bigint(20) NOT NULL DEFAULT 0 comment '最后更新时间',
  PRIMARY KEY (id),
  unique key uniq_idx_name(name)
) ENGINE=InnoDB default charset=utf8mb4 comment 'leader选举';

CREATE TABLE IF NOT EXISTS `datasource_config` (
  `id` int(10) unsigned not null auto_increment comment '主键id',
  `url` varchar(128) not null default '' comment 'jdbc url',
  `user_name` varchar(100) NOT NULL default '' comment 'db username',
  `password` varchar(100) NOT NULL DEFAULT '' comment 'db password',
  `status` TINYINT NOT NULL DEFAULT 0 COMMENT 'db状态',
  `room`           VARCHAR(20)      NOT NULL DEFAULT '' COMMENT '机房',
  `create_time` TIMESTAMP NOT NULL DEFAULT '2018-01-01 01:01:01' COMMENT '创建时间',
  `update_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  PRIMARY KEY (id),
  unique key uniq_idx_name(url)
) ENGINE=InnoDB default charset=utf8mb4 comment '客户端db配置表';

CREATE TABLE IF NOT EXISTS `readonly_broker_group_setting` (
  `id`           BIGINT(11) UNSIGNED NOT NULL  AUTO_INCREMENT COMMENT '自增主键',
  `subject`      VARCHAR(100)        NOT NULL DEFAULT '' COMMENT '针对哪个主题，可以使用*表示全部',
  `broker_group` VARCHAR(20)         NOT NULL DEFAULT '' COMMENT '需要标记为readonly的组',
  `create_time`  TIMESTAMP           NOT NULL  DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',

  PRIMARY KEY (`id`),
  UNIQUE `uniq_subject_broker_group` (`subject`, `broker_group`)
)ENGINE InnoDB DEFAULT CHARSET = utf8mb4 COMMENT '只读broker group标记表';

CREATE TABLE IF NOT EXISTS `client_meta_info`
(
  `id`             INT(11) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `subject_info`   VARCHAR(100)     NOT NULL DEFAULT '' COMMENT '主题',
  `client_type`    TINYINT          NOT NULL DEFAULT 0 COMMENT 'client类型',
  `consumer_group` VARCHAR(100)     NOT NULL DEFAULT '' COMMENT '消费组',
  `client_id`      VARCHAR(100)     NOT NULL DEFAULT '' COMMENT 'client id',
  `app_code`       VARCHAR(100)     NOT NULL DEFAULT '' COMMENT '应用',
  `room`           VARCHAR(20)      NOT NULL DEFAULT '' COMMENT '机房',
  `online_status`  VARCHAR(10)      NOT NULL DEFAULT 'ONLINE' COMMENT '在线状态',
  `create_time`    TIMESTAMP        NOT NULL DEFAULT '1970-01-01 08:00:01' COMMENT '创建时间',
  `update_time`    TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_subject_client_type_consumer_group_client_id` (`subject_info`, `client_type`, `consumer_group`, `client_id`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COMMENT = '订阅关系表';

CREATE TABLE `partition` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
  `subject` varchar(100) COLLATE utf8mb4_bin NOT NULL COMMENT '主题',
  `partition_id` int(11) NOT NULL COMMENT '物理分区',
  `partition_name` varchar(110) COLLATE utf8mb4_bin NOT NULL COMMENT '分区名',
  `logical_partition_lower_bound` int(11) COLLATE utf8mb4_bin NOT NULL COMMENT '逻辑分区范围下界, 闭区间, 如 [0, 500)',
  `logical_partition_upper_bound` int(11) COLLATE utf8mb4_bin NOT NULL COMMENT '逻辑分区范围上界, 开区间, 如 [0, 500)',
  `broker_group` varchar(45) COLLATE utf8mb4_bin NOT NULL COMMENT '物理分区所在的 broker',
  `status` varchar(10) COLLATE utf8mb4_bin NOT NULL COMMENT '状态, RW, R',
  `create_ts` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_ts` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_subject_physical_partition` (`subject`,`physical_partition`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `partition_set` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `subject` varchar(100) COLLATE utf8mb4_bin NOT NULL COMMENT '主题',
  `physical_partitions` varchar(4096) COLLATE utf8mb4_bin NOT NULL COMMENT '物理分区, 逗号分隔, 如 "1,2,3"',
  `version` int(11) NOT NULL COMMENT '扩容/缩容版本号, 该版本只有扩容缩容会变更',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_subject_version` (`subject`,`version`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='分区版本';

CREATE TABLE `partition_allocation` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
  `subject` varchar(100) COLLATE utf8_bin NOT NULL COMMENT '主题',
  `consumer_group` varchar(128) COLLATE utf8_bin NOT NULL COMMENT 'consumerGroup id',
  `allocation_detail` varchar(16384) COLLATE utf8_bin NOT NULL COMMENT '该 consumerGroup 下所有 client 分配的物理分区, JSON',
  `partition_set_version` int(11) NOT NULL COMMENT '扩容/缩容版本号, 该版本只有扩容缩容会变更',
  `version` int(11) NOT NULL COMMENT '本次分配的版本号, 用于做分配乐观锁',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_subject_consumer_group` (`subject`, `consumer_group`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='分区分配详情';