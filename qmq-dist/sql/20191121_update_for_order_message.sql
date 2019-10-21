ALTER TABLE `wmq_meta_server`.`client_meta_info`
ADD COLUMN `consume_strategy` VARCHAR(100) NOT NULL DEFAULT 'SHARED' COMMENT '消费模式' AFTER `room`,
ADD COLUMN `online_status` VARCHAR(10) NOT NULL DEFAULT 'ONLINE' COMMENT '在线状态' AFTER `consume_strategy`;

CREATE TABLE `partitions` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
  `subject` varchar(100) COLLATE utf8mb4_bin NOT NULL COMMENT '主题',
  `partition_id` int(11) NOT NULL COMMENT '物理分区',
  `partition_name` varchar(110) COLLATE utf8mb4_bin NOT NULL COMMENT '分区名',
  `logical_partition_lower_bound` int(11) COLLATE utf8mb4_bin NOT NULL COMMENT '逻辑分区范围下界, 闭区间, 如 [0, 500)',
  `logical_partition_upper_bound` int(11) COLLATE utf8mb4_bin NOT NULL COMMENT '逻辑分区范围上界, 开区间, 如 [0, 500)',
  `broker_group` varchar(45) COLLATE utf8mb4_bin NOT NULL COMMENT '物理分区所在的 broker',
  `rw_status` varchar(10) COLLATE utf8mb4_bin NOT NULL COMMENT '状态, RW, R',
  `create_ts` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_ts` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_subject_physical_partition` (`subject`,`partition_id`)
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
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_subject_consumer_group` (`subject`,`consumer_group`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='分区分配详情';
