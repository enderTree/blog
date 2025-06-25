优化前：
```sql
SELECT 
        /*+ mapjoin(dd),skewjoin(a(url))*/
        a.* except(url_tag, join_key),
        CASE 
            WHEN cc.url IS NOT NULL THEN 'black_url'
            WHEN dd.url IS NOT NULL THEN 'black_url'
            ELSE ''
        END AS url_tag
    FROM
    (
        select 
            *,
            1 AS join_key
        from 
            dwd_crawler_urls_step1_di a 
        where 
            ymd = '${P_DATE}'
            AND url_tag != 'black_url'
    ) as
    LEFT JOIN 
        black_urls2 cc  
        ON replace(a.host, 'www.', '') = replace(cc.url, 'www.', '')
	LEFT JOIN 
        black_urls1 dd  
        ON INSTR(a.url, dd.url) > 0
```
优化后
```sql
SELECT 
        /*+ mapjoin(dd),skewjoin(a(url))*/
        a.* except(url_tag, join_key),
        CASE 
            WHEN cc.url IS NOT NULL THEN 'black_url'
            WHEN dd.url IS NOT NULL THEN 'black_url'
            ELSE ''
        END AS url_tag
    FROM
    (
        select 
            *,
            1 AS join_key
        from 
            dwd_crawler_urls_step1_di a 
        where 
            ymd = '${P_DATE}'
            AND url_tag != 'black_url'
    ) a
    LEFT JOIN 
        black_urls1 dd  
        ON INSTR(a.url, dd.url) > 0
    LEFT JOIN 
        black_urls2 cc  
        ON replace(a.host, 'www.', '') = replace(cc.url, 'www.', '')
```
优化前执行时间

<img width="991" alt="Image" src="https://github.com/user-attachments/assets/3d62cdfd-b1e0-4579-adc9-a8bc19ac64ff" />

<img width="872" alt="Image" src="https://github.com/user-attachments/assets/46033d2f-9fbe-4400-b6c6-489f97ee7f00" />

优化后执行时间

<img width="978" alt="Image" src="https://github.com/user-attachments/assets/74de5fc3-08ff-4ed6-8c94-9b0d31c18799" />

<img width="944" alt="Image" src="https://github.com/user-attachments/assets/b2f397da-420d-484b-9655-3dda6ee57046" />
