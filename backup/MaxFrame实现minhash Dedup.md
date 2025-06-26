```python
import re
from itertools import tee
from typing import Iterable, Set, List

import numpy as np
import maxframe.dataframe as md
from maxframe import options
from maxframe.session import new_session
from maxframe.udf import with_python_requirements
from maxframe.learn.contrib.graph import connected_components
from odps import ODPS

import logging

logging.basicConfig(level=logging.INFO)

SEED = 42
NON_ALPHA = re.compile("[^A-Za-z_0-9]")
RNG = np.random.RandomState(SEED)
MAX_HASH = np.uint64((1 << 32) - 1)
MERSENNE_PRIME = np.uint64((1 << 61) - 1)


# 通过相似度阈值和哈希函数个数，生成条带数量和条带行数参数
def optimal_param(
    threshold: float,
    num_perm: int,
    false_positive_weight: float = 0.5,
    false_negative_weight: float = 0.5,
):
    from scipy.integrate import quad as integrate

    def false_positive_probability(threshold: float, b: int, r: int):
        """Source: `datasketch.lsh`"""

        def proba(s):
            return 1 - (1 - s ** float(r)) ** float(b)

        a, _ = integrate(proba, 0.0, threshold)
        return a

    def false_negative_probability(threshold: float, b: int, r: int):
        """Source: `datasketch.lsh`"""

        def proba(s):
            return 1 - (1 - (1 - s ** float(r)) ** float(b))

        a, _ = integrate(proba, threshold, 1.0)
        return a

    min_error = float("inf")
    opt = (0, 0)
    for b in range(1, num_perm + 1):
        max_r = int(num_perm / b)
        for r in range(1, max_r + 1):
            fp = false_positive_probability(threshold, b, r)
            fn = false_negative_probability(threshold, b, r)
            error = fp * false_positive_weight + fn * false_negative_weight
            if error < min_error:
                min_error = error
                opt = (b, r)
    return opt


# 依赖 xxhash 完成 hash
@with_python_requirements("xxhash")
def generate_hash_values(row):
    idx = str(row[0])
    content = row[1]

    print(idx, content[:10], flush=True)

    import xxhash

    def ngrams(sequence: List[str], n: int, min_length: int = 5) -> Iterable:
        if len(sequence) < min_length:
            return []

        iterables = tee(sequence, n)
        for i, sub_iterable in enumerate(iterables):
            for _ in range(i):
                next(sub_iterable, None)
        return zip(*iterables)

    def ngram_hashes(content: str, n: int, min_length: int = 5) -> Set[int]:
        import xxhash

        # ========== edit ===========
        tokens: List[str] = NON_ALPHA.split(content.lower())
        ng: set[bytes] = {
            bytes(" ".join(t).lower(), "utf-8") for t in ngrams(tokens, n, min_length)
        }
        return {xxhash.xxh32_intdigest(n) for n in ng}

    # 获取 hash 生成的排列的参数
    a, b = PERMUTATIONS

    # 获得 ngram hash
    hashes = np.array(
        list(ngram_hashes(content, args_ngram_size, args_min_length)), dtype=np.uint64
    )

    # 如果长度不足，不进行相似度判定
    if len(hashes) == 0:
        return

    # 通过算法为每个 ngram 生成多个 hash 值
    chunk_size = 100
    min_hashes = np.full(args_num_perm, MAX_HASH)
    for i in range(0, len(hashes), chunk_size):
        chunk = hashes[i : i + chunk_size]
        p_hashes = ((np.outer(chunk, a) + b) % MERSENNE_PRIME) & MAX_HASH
        min_hashes = np.minimum(min_hashes, p_hashes.min(axis=0))

    # 生成条带的 hash
    Hs = [bytes(min_hashes[start:end].byteswap().data) for start, end in HASH_RANGES]

    # 返回结果，结果为每个条带返回一行数据
    for band_idx, H in enumerate(Hs):
        yield band_idx, H.hex(), idx

mapper_memory = 4096
reducer_memory = 12288
joiner_memory = 12288
mapper_split_size = 1024
instance_num = 50000

input_table = "xxx"
output_table = "xxxx1"

# content_hash
process_column = "content_hash"  #之后需要改为content
lifecycle = 365
partitions = None  # or None

# 相似度阈值
args_threshold = 0.6  # 0.6
# 文本长度限制
args_min_length = 5  # 5
args_ngram_size = 5  # 5
# hash 函数个数
args_num_perm = 256  # 256

# 关闭查询加速，在这个场景计算时间偏长，不适用mcqa
options.sql.enable_mcqa = False

sql_settings = {
    "odps.namespace.schema": "true",
    "odps.session.image": "common",
    "odps.function.timeout": 3600,
    "odps.sql.mapper.memory": mapper_memory,  # 弹内
    "odps.stage.mapper.mem": mapper_memory,  # 弹外
    "odps.sql.reducer.memory": reducer_memory,  # 弹内
    "odps.stage.reducer.mem": reducer_memory,  # 弹外
    "odps.sql.joiner.memory": joiner_memory,  # 弹内
    "odps.stage.joiner.mem": joiner_memory,  # 弹外
    "odps.stage.mapper.split.size": mapper_split_size,
    "odps.sql.cfile2.field.maxsize": 268435456,
    "odps.task.merge.enabled": False,
    "odps.sql.allow.fullscan": "true",
    "odps.dag2.compound.config": "fuxi.worker.reuse.policy:NO_REUSE",
    "odps.sql.window.function.newimpl": "false",
}

# 设置最大列的大小，并发
if instance_num:
    sql_settings.update(
        {
            "odps.sql.split.dop": {input_table: int(instance_num)},
            "odps.stage.reducer.num": 5000,
            "odps.stage.joiner.num": 5000,
            "odps.sql.sys.flag.fuxi_MaxTaskWorkerCount": int(instance_num),
            "odps.sql.max.desired.parallelism": int(instance_num),
            "odps.job.workers.limit": int(instance_num),
        }
    )
options.sql.settings = sql_settings
options.dag.settings.update({"codegen.groupby_first_value_allowed": "true"})

o = ODPS(
    access_id="",
    secret_access_key="",
    project="xxx",
    # endpoint="http://service.cn-hangzhou.maxcompute.aliyun.com/api",
    endpoint="https://service.cn-hangzhou-vpc.maxcompute.aliyun-inc.com/api",
)

session = new_session(o)
print("session id: ", session.session_id, flush=True)
print("session logview: ", session.get_logview_address(), flush=True)

B, R = optimal_param(args_threshold, args_num_perm)

# 生成 HashRange
HASH_RANGES = [(i * R, (i + 1) * R) for i in range(B)]

# 生成 Hash 函数参数
PERMUTATIONS = np.array(
    [
        (
            RNG.randint(1, MERSENNE_PRIME, dtype=np.uint64),
            RNG.randint(0, MERSENNE_PRIME, dtype=np.uint64),
        )
        for _ in range(args_num_perm)
    ],
    dtype=np.uint64,
).T

df = md.read_odps_table(input_table, partitions=partitions)

# 直接对相同文本去重
df = df.drop_duplicates(subset=[process_column], keep="first")

# # 过滤 text 为空的行, 生成 index 列
df = df.reset_index(drop=False).rename(columns={"index": "_doc_id"})
# 触发计算并缓存结果, 通过 display 预览
df.execute()

# # 投影出所需的数据
docid_docinfo_df = df[["_doc_id", process_column]]

# # 应用 udtf 生成所有的 MinHash 和 LSH
hash_df = docid_docinfo_df.mf.flatmap(
    generate_hash_values,
    dtypes={"band_idx": np.int_, "hash": np.str_, "_doc_id": np.str_},
    raw=True,
)

hash_df_temp = "tmp_hash_df"
md.to_odps_table(
    hash_df, hash_df_temp, index=False, overwrite=True, lifecycle=7
).execute()

components_df = md.read_odps_query(
    f"""
    select distinct similar_docs as index, similar_docs as id, _doc_id as component from (
        select cast (_doc_id as bigint) as similar_docs,
                            MIN(cast (_doc_id as bigint)) over(PARTITION BY band_idx, hash) as _doc_id,
                            RANK() over (PARTITION BY band_idx,hash ORDER BY cast (_doc_id as bigint) ASC) as rnk from {hash_df_temp}
    ) where rnk > 1
    """,
    index_col="index",
)
converged = False

round = 0
# 计算完整联通分量
while not converged:
    round = round + 1
    components_df, converged_scalar = connected_components(
        components_df, "id", "component", 6
    )
    session.execute(components_df, converged_scalar)
    converged = converged_scalar.fetch()

components_df = components_df.set_index("id")
result_df = df.merge(components_df, left_on="_doc_id", right_index=True, how="left")
result_df = result_df[result_df["component"].isnull()]
result_df = result_df.drop(["component", "_doc_id"], axis=1)

md.to_odps_table(
    result_df, output_table, lifecycle=lifecycle, overwrite=True, index=False
).execute()

session.destroy()
```