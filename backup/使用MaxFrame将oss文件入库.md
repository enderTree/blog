1. oss配置
```python
OSS_ACCESS_ID = ""  # OSS 的 Access ID
OSS_SECRET_ACCESS_KEY = ""  # OSS 的 Secret ID

# 填写 Bucket 信息
OSS_INTERNET_ENDPOINT = (
    "oss-cn-shanghai.aliyuncs.com"  # OSS 的公网访问 endpoint，您也可以使用 sts token
)
OSS_INTERNAL_ENDPOINT = "oss-cn-shanghai-internal.aliyuncs.com"  # OSS 的公网访问 endpoint，您也可以使用 sts token
OSS_BUCKET_NAME = "crawler-data-storage"  # OSS Bucket 的名字
OSS_BUCKET_ENDPOINT = f"{OSS_BUCKET_NAME}.{OSS_INTERNET_ENDPOINT}:80"
OSS_BUCKET_ENDPOINT

import oss2
from oss2 import defaults
defaults.connection_pool_size = 50

auth = oss2.Auth(OSS_ACCESS_ID, OSS_SECRET_ACCESS_KEY)
bucket = oss2.Bucket(auth, OSS_INTERNET_ENDPOINT, OSS_BUCKET_NAME)
```
2. 定义oss文件扫描方法
```python
import oss2
from concurrent.futures import ThreadPoolExecutor, as_completed


def get_oss_files(bucket, prefix, ext=".snappy.parquet", max_size=1 * 1024 * 1024 * 1024 * 1024):
    """
    使用多线程遍历 OSS 路径下的所有目录。
    """
    sub_folders = []
    # print(ext)L

    # 创建一个线程池
    with ThreadPoolExecutor(max_workers=10) as executor:
        # 用于存储子目录任务的 future
        futures = []

        for obj in oss2.ObjectIteratorV2(bucket=bucket, prefix=prefix, delimiter="/"):
            if obj.is_prefix():  # 判断 obj 是否为文件夹
                # print("directory: " + obj.key)
                # 提交任务到线程池，每个子目录由单独的线程处理
                futures.append(
                    executor.submit(
                        get_oss_files,
                        bucket=bucket,
                        prefix=obj.key,
                        ext=ext,
                        max_size=max_size,
                    )
                )
            elif obj.key.endswith(ext) and obj.size < max_size:
                sub_folders.append(obj.key)

        # 收集所有线程返回的结果
        for future in as_completed(futures):
            sub_folders.extend(future.result())

    return sub_folders
```
3. 创建maxframe session
```python
from maxframe.session import new_session
import maxframe.dataframe as md
from maxframe.udf import with_resource_libraries
from maxframe import options

import pandas as pd
import numpy as np
from odps import ODPS

import logging

logging.basicConfig(level=logging.INFO)

o = ODPS(
    access_id="",
    secret_access_key="",
    project="crawler",
    endpoint="http://service.cn-shanghai.maxcompute.aliyun.com/api",
)

from maxframe import options

options.sql.settings = {
    # "odps.sql.split.dop": '{"maxframe_test_bj.book3_file_list":100}',
}


session = new_session(o)
print(session.session_id)
```
4. 开始扫描数据
这里量太大了 我分批次扫的，可以自己改造下
```python
import gc
import pandas as pd
files = []
for i in range(60):
    if i<=40:
        continue
    batch = str(i).rjust(2, '0')
    prefix=f"xxx/{batch}/output/"
    print(prefix)
    gc.collect()
    files.extend(get_oss_files(bucket=bucket, prefix=prefix, ext=".snappy.parquet"))
# pd_df = pd.DataFrame(files, columns=["oss_file"])

# df = md.read_pandas(pd_df)
# P_DATE = '2025-05-27'
# execute_info = df.to_odps_table("ods_cc_url_oss_path_df", partition=f'ymd={P_DATE},batch={batch}', overwrite=True)
# execute_info.execute()
```
5. 路径数据入表
这一步可以不做，但为了调试方便 可以先入库
```python
pd_df = pd.DataFrame(files, columns=["oss_file"])

df = md.read_pandas(pd_df)
P_DATE = '2025-05-27'
batch='xxx'
execute_info = df.to_odps_table("ods_cc_url_oss_path_df", partition=f'ymd={P_DATE},batch={batch}', overwrite=True)
execute_info.execute()
```
6. 定义maxframe oss下载代码
```python
# OSS Related Functions
def get_error_str():
    import traceback, sys

    exc_type, exc_value, exc_traceback = sys.exc_info()
    traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
    return "\n".join(traceback_details)


def create_oss_bucket(ak, sk, endpoint, bucket):
    print(f"create ", flush=True)

    try_time = 10
    while try_time > 0:
        try:
            import time
            import oss2
            auth = oss2.Auth(ak, sk)
            bucket = oss2.Bucket(auth, endpoint, bucket)
            print(f"get bucket success {try_time}", flush=True)
            return bucket
        except:
            print(f"error: {get_error_str()}", flush=True)
            try_time -= 1
    print("get bucket failed", flush=True)
    return


def download_from_oss(
    bucket,
    oss_file_path,
    local_path,
    limit_speed=53687091 * 2,
    retry_times=5,
):
    import oss2
    from oss2.models import OSS_TRAFFIC_LIMIT

    while True:
        try:

            headers = {OSS_TRAFFIC_LIMIT: str(limit_speed)}

            bucket.get_object_to_file(
                oss_file_path,
                local_path,
                headers=headers,
                progress_callback=lambda uploaded, total: print(
                    f"downloaded {uploaded} bytes of {total} bytes, {uploaded / total * 100}%",
                    flush=True,
                ),
            )
            return True
        except Exception as e:
            print(
                "OSS download failed, retrying, rest retry times {retry_times}, Error: {e}",
                flush=True,
            )
            print(f"error: {get_error_str()}", flush=True)
            retry_times -= 1
            if retry_times <= 0:
                break
    return False
```
7. 下载和解析
```python
from maxframe.udf import with_python_requirements, with_running_options


# 下载和解析
@with_python_requirements("fastparquet", "oss2")
@with_running_options(memory=24)
def extract_from_parquet(row, _ctx={}):
    import gc
    import time
    import fastparquet
    import sys

    def get_oss_client():
        if "oss_client" not in _ctx:
            _ctx["oss_client"] = create_oss_bucket(
                OSS_ACCESS_ID,
                OSS_SECRET_ACCESS_KEY,
                OSS_INTERNAL_ENDPOINT,
                OSS_BUCKET_NAME,
            )
        return _ctx["oss_client"]

    def _download_file(oss_client, file_url, target_file_name):
        print(f"downloading to {target_file_name}")
        if not download_from_oss(oss_client, file_url, target_file_name):
            raise ValueError(f"Failed to download file from oss {file_url}")

    def _get_file_name(file_url):
        return file_url.split("/")[-1]

    def _forward_parquet(parquet_file: str):
        pf = fastparquet.ParquetFile(parquet_file)
        for df in pf.iter_row_groups(columns=['url']):
            gc.collect()
            for url in df['url']:
                yield url
        print("forwarding done", flush=True)

    # 从一行中读取 oss_file
    file_url = row["oss_file"]
    import gc

    gc.collect()

    print(f"download from {file_url}", flush=True)
    file_name = _get_file_name(file_url)
    try:
        local_file_path = f"./{file_name}"

        oss_client = get_oss_client()
        _download_file(oss_client, file_url, local_file_path)

        gc.collect()
        for rows in _forward_parquet(local_file_path):
            # drop the first column for this case
            yield rows
    except:
        print(f"ERROR: {sys.exc_info()[0]}", flush=True)
        raise
    finally:
        import os

        if os.path.exists(file_name):
            os.remove(file_name)
```
8. 执行
```python
df = md.read_odps_table("ods_cc_url_oss_path_df", partition=f'ymd={P_DATE}')
result_df = df.mf.flatmap(
    extract_from_parquet,
    dtypes={
        "url": np.str_
    },
)
result_df.to_odps_table("ods_cc_urls_df", partition=f'ymd={P_DATE}', overwrite=True).execute()
```
