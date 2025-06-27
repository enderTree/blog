### 1. range分区
1. 建表
```sql
CREATE TABLE `dws_xingye_discovery_page_stats_usr_di` (
    `ymd` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
    user_id varchar(100) COMMENT '用户ID',
    platform varchar(50) COMMENT '平台',
    is_new varchar(20) COMMENT '是否新用户',
    sub_tab_name varchar(128) COMMENT '发现页tab',
    page_view_cnt BIGINT COMMENT '发现页曝光PV',
    search_topic_npc_view_cnt BIGINT COMMENT '发现页NPC曝光PV',
    search_topic_npc_view_npc_cnt BIGINT COMMENT '发现页NPC曝光npc数',
    search_topic_npc_click_cnt BIGINT COMMENT '发现页NPC点击PV', 
    search_topic_npc_click_npc_cnt BIGINT COMMENT '发现页NPC点击npc数',
    message_send_npc_cnt BIGINT COMMENT '消息发送npc次数',
    message_send_message_cnt BIGINT COMMENT '消息发送消息次数'
) ENGINE=OLAP
DUPLICATE KEY(`ymd`,`user_id`,`platform`,`is_new`,`sub_tab_name`)
PARTITION BY RANGE(`ymd`)()
DISTRIBUTED BY HASH(`user_id`) BUCKETS 10
PROPERTIES (
    "replication_allocation" = "tag.location.default: 3", -- 副本分配策略，在标签为default的位置保存3个副本
    "min_load_replica_num" = "-1", -- 最小导入副本数，-1表示使用默认值（通常等于副本数）
    "dynamic_partition.enable" = "true", -- 启用动态分区功能
    "dynamic_partition.time_unit" = "DAY", -- 分区时间单位为天（DAY）
    "dynamic_partition.time_zone" = "Asia/Shanghai", -- 时区设置为上海时区（UTC+8）
    "dynamic_partition.start" = "-15", -- 保留当前日期前15天的分区
    "dynamic_partition.end" = "7", -- 提前创建未来7天的分区
    "dynamic_partition.prefix" = "p_", -- 分区名前缀，生成的分区名如p_20250126
    "dynamic_partition.replication_allocation" = "tag.location.default: 3", -- 动态创建的分区也保持3副本
    "dynamic_partition.buckets" = "10", -- 每个动态分区的bucket数量为10
    "dynamic_partition.create_history_partition" = "false", -- 不创建历史分区（只创建当前和未来分区）
    "dynamic_partition.history_partition_num" = "-1", --  -1表示不限制历史分区数量
    "dynamic_partition.hot_partition_num" = "0", -- 0表示没有特别的热分区设置
    "dynamic_partition.reserved_history_periods" = "NULL", -- 保留的历史周期，NULL表示使用默认设置
    "dynamic_partition.storage_policy" = "", -- 存储策略为空（使用默认）
    "storage_medium" = "hdd", -- 存储介质为机械硬盘（hdd）
    "storage_format" = "V2", -- 使用V2存储格式
    "light_schema_change" = "true", -- 启用轻量级schema变更
    "disable_auto_compaction" = "false", -- 不禁用自动压缩
    "enable_single_replica_compaction" = "false", -- 不启用单副本压缩
    "group_commit_interval_ms" = "10000", -- 组提交时间间隔10秒
    "group_commit_data_bytes" = "134217728" -- 组提交数据大小阈值128MB
);
```
2. 临时开启历史分区创建
```sql
ALTER TABLE dws_xingye_discovery_page_stats_usr_di SET (
    "dynamic_partition.start" = "-90",
    "dynamic_partition.create_history_partition" = "true"
);
-- 等待一段时间 （10分钟左右）
SHOW PARTITIONS FROM dws_xingye_discovery_page_stats_usr_di;

ALTER TABLE dws_xingye_discovery_page_stats_usr_di SET (
    "dynamic_partition.create_history_partition" = "false"
);
```
### 1. list分区
```sql
CREATE TABLE `dws_xingye_discovery_page_stats_usr_di` (
    ymd varchar(50) NULL,
    user_id varchar(100) COMMENT '用户ID',
    platform varchar(50) COMMENT '平台',
    is_new varchar(20) COMMENT '是否新用户',
    sub_tab_name varchar(128) COMMENT '发现页tab',
    page_view_cnt BIGINT COMMENT '发现页曝光PV',
    search_topic_npc_view_cnt BIGINT COMMENT '发现页NPC曝光PV',
    search_topic_npc_view_npc_cnt BIGINT COMMENT '发现页NPC曝光npc数',
    search_topic_npc_click_cnt BIGINT COMMENT '发现页NPC点击PV', 
    search_topic_npc_click_npc_cnt BIGINT COMMENT '发现页NPC点击npc数',
    message_send_npc_cnt BIGINT COMMENT '消息发送npc次数',
    message_send_message_cnt BIGINT COMMENT '消息发送消息次数'

) ENGINE=OLAP
DUPLICATE KEY(`ymd`,`user_id`,`platform`,`is_new`,`sub_tab_name`)
PARTITION BY LIST (`ymd`)
(
  PARTITION p20250626 VALUES IN ('2025-06-26')
)
PROPERTIES (
    "file_cache_ttl_seconds" = "0",
    "is_being_synced" = "false",
    "storage_medium" = "hdd",
    "storage_format" = "V2",
    "inverted_index_storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false",
    "group_commit_interval_ms" = "10000",
    "group_commit_data_bytes" = "134217728"
);
delete from  dws_xingye_discovery_page_stats_usr_di where ymd = "${P_DATE}" ;
alter table dws_xingye_discovery_page_stats_usr_di add partition p_${p_date} VALUES IN (("${P_DATE}"))
```
