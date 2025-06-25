### 1. 建表
1. json格式

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS dialogue.dwd_ext_xingye_log_chatml_data(
    `data` STRING
) 
PARTITIONED BY (
    data_type STRING,
    ymd STRING) 
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.mapred.TextOutputFormat' 
LOCATION 'oss://data-dlc/minimax-dialogue/data/chatml/';
```

2. parquet格式
   ```sql
   CREATE EXTERNAL TABLE dwd_crawler_html_out_di
   (
      url string,
       html string,
       source_tag string,
       lang string
   )
   partitioned BY (ymd string)
   stored AS parquet  
   location 'oss://data-dlc/minimax-dialogue/data/chatml/data_type=html_data/';
   ```
3. 自定义解析器
```sql
CREATE EXTERNAL TABLE ods_crawler_sites_warc_data_extract_result_external_hi (
    warc_host string,
    meta_info string,
    final_url string,
    status_code string,
    data_dt string,
    biz_code string,
    target_url string,
    warc_date string,
    content_type string,
    content_length string,
    warc_file_name string,
    content string,
    import_msg string
)
PARTITIONED BY (
    host string,
    status string,
    ymd string,
    hour string
)
stored BY 'com.crawler.udf.sites_warc.WarcStorageHandler' 
with serdeproperties (
    'max_file_size' = '157286400'
) 
LOCATION 'oss://crawler-data-storage/Crawlerlee/'
USING 'crawler_udf-1.0-SNAPSHOT.jar'
```
可参考这篇文章

### 2. 使用
1. 插入数据
```sql
SET odps.sql.unstructured.oss.commit.mode = true; -- 防止生成.odps文件夹
INSERT OVERWRITE TABLE dwd_crawler_html_out_di PARTITION(ymd='${P_DATE}')
SELECT 
    a.source_url,
    html,
    biz_code,
    parse_lang
FROM 
    dwd_crawler_content_filtered_detail_byscore_di a
WHERE 
    a.ymd = '${P_DATE}'
    AND a.score_level != 'low'
    AND COALESCE(html, '') != ''
```
2. 读取数据
```
-- 扫描所有路径
msck TABLE ods_crawler_sites_warc_data_extract_result_external_hi ADD partitions;
-- 扫描指定路径
ALTER TABLE ods_crawler_sites_warc_data_extract_result_external_hi ADD IF NOT EXISTS 
PARTITION (host='demo.com',status='error',ymd='20250624', hour='09') 
PARTITION (host='demo.com',status='success',ymd='20250624', hour='09')
;
-- 指定location
ALTER TABLE log_table_external ADD PARTITION (year = '2016', month = '06', day = '01')
location 'oss://oss-cn-hangzhou-internal.aliyuncs.com/bucket名称/oss-odps-test/log_data_customized/2016/06/01/';
-- 指定路径映射
MSCK REPAIR TABLE  ods_crawler_sites_warc_data_extract_result_external_hi ADD PARTITIONS
WITH PROPERTIES ('odps.msck.partition.column.mapping'='host:host,status:status,ymd:ymd');
```