CREATE TABLE IF NOT EXISTS `qmq_dic`
(
    `id`   INT(11) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
    `name` varchar(100)     NOT NULL default '' comment '字典名称',
    PRIMARY KEY (id),
    unique key uniq_idx_name(name)
)ENGINE=InnoDB default charset=utf8mb4 comment 'qmq字典表';