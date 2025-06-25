```sql

SELECT
    *
FROM
(

    SELECT
        tag,
        doc_cnt,
        (doc_cnt/SUM(doc_cnt) OVER())*100
    FROM
    (
        SELECT 
            tag,
            SUM(doc_cnt) AS doc_cnt
        FROM
            dwd_crawler_host_classification_di a
        LEFT ANTI JOIN 
            tmp_gumu_data_0624_2 b
            ON a.host = b.host
        WHERE
            ymd = '2025-06-23'
            AND tag_type = 'topic'
        GROUP BY 
            tag
    )
)
WHERE 
    tag = 'Advertising_&_Marketing'
;
```