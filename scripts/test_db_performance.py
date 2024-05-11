#!/usr/bin/env python3

import shutil
from pyarrow.parquet import ParquetFile
import pandas as pd
import aimrocks
import time
import os

test_get = True
test_put = False
parquet_path = "/disk2/federico/the-stack/the-stack-small_1M.parquet"
content_db_paths = [
    # "/disk2/federico/the-stack/rocksdb-sha_content-zstd-4Mblock"
    # "/disk2/federico/the-stack/rocksdb-sha_content-zstd-noblock",
    # "/disk2/federico/the-stack/rocksdb-sha_content-nocomp-4Mblock",
    # "/disk2/federico/the-stack/rocksdb-sha_content-nocomp-noblock",
    # "/disk2/federico/the-stack/rocksdb-sha_content-nocomp-noblock-1Kcontent",
    # "/disk2/federico/the-stack/rocksdb-sha_content-nocomp-noblock-10Kcontent",
    "/disk2/federico/rocksdb_perf_test/db-nocomp-256K_block",
    "/disk2/federico/rocksdb_perf_test/db-snappy-256K_block",
    "/disk2/federico/rocksdb_perf_test/db-zlib-256K_block",
    "/disk2/federico/rocksdb_perf_test/db-zstd-256K_block",
    # "/disk2/federico/rocksdb_perf_test/db-lz4-256K_block",
    "/disk2/federico/rocksdb_perf_test/db-nocomp-4K_block",
    "/disk2/federico/rocksdb_perf_test/db-snappy-4K_block",
    "/disk2/federico/rocksdb_perf_test/db-zlib-4K_block",
    "/disk2/federico/rocksdb_perf_test/db-zstd-4K_block",
    # "/disk2/federico/rocksdb_perf_test/db-lz4-4K_block",
    "/disk2/federico/rocksdb_perf_test/db-nocomp-4M_block",
    "/disk2/federico/rocksdb_perf_test/db-snappy-4M_block",
    "/disk2/federico/rocksdb_perf_test/db-zlib-4M_block",
    "/disk2/federico/rocksdb_perf_test/db-zstd-4M_block",
    # "/disk2/federico/rocksdb_perf_test/db-lz4-4M_block",
]
test_db_dir = "/disk2/federico/db/tmp"

KiB = 1024
MiB = 1024 * 1024

if __name__ == "__main__":
    print(f"Starting at {time.asctime()}, pid: {os.getpid()}\n")

    pf = ParquetFile(parquet_path)
    dataframes = []
    for batch in pf.iter_batches(columns=["hexsha", "content"]):
        batch_df = batch.to_pandas()
        dataframes.append(batch_df)
    df = pd.concat(dataframes, ignore_index=True)

    # test get performance
    print(f"GET test, reading from {parquet_path}\n")
    print(
        "CONTENT_DB,SINGLE_TOT_TIME(s),TIME/GET(s),PERF(MiB/s),MULTI_TOT_TIME(s),TIME/GET(s),PERF(MiB/s)"
    )
    if test_get:
        for content_db_path in content_db_paths:
            print(f"{content_db_path.split('/')[-1]},", end="")
            opts = aimrocks.Options()
            opts.max_open_files = 40000
            opts.create_if_missing = False
            contents_db = aimrocks.DB(content_db_path, opts, read_only=True)
            tot_get_time = 0
            tot_get_size = 0
            shas = []
            # test single gets
            for _, row in df.iterrows():
                sha = str(row["hexsha"])
                shas.append(str.encode(sha))
                get_start = time.time()
                content = contents_db.get(str.encode(sha))
                get_end = time.time()
                tot_get_size += len(content)
                tot_get_time += get_end - get_start
            total_get_size_mb = tot_get_size / MiB
            print(
                f"{round(tot_get_time, 3)},{round(tot_get_time/len(df), 5)},{round(total_get_size_mb / tot_get_time, 3)},",
                end="",
            )
            # test multi-gets in chunks of 100
            tot_get_time = 0
            for i in range(0, len(shas), 100):
                j = min(i + 10, len(shas))
                get_start = time.time()
                out = contents_db.multi_get(shas[i:j])
                get_end = time.time()
                tot_get_time += get_end - get_start
            print(
                f"{round(tot_get_time, 3)},{round(tot_get_time/len(df), 5)},{round(total_get_size_mb / tot_get_time, 3)}"
            )

    block_sizes = [
        4 * KiB,
        256 * KiB,
        4 * MiB,
    ]
    compressions = [
        (aimrocks.CompressionType.no_compression, "nocomp"),
        (aimrocks.CompressionType.zstd_compression, "zstd"),
    ]

    # test put performance
    if test_put:
        print(f"\nPUT test, creating DBs in {test_db_dir}\n")
        print(
            "BLOCK_SIZE(KiB),COMPRESSION,TOT_PUT_SIZE(KiB),TOT_PUT_TIME(s),AVG_PUT_TIME(s),PERFORMANCE(KiB/s)"
        )
        for block_size in block_sizes:
            for compression in compressions:
                print(f"{block_size / KiB},", end="")
                compr = compression[0]
                compr_str = compression[1]
                print(f"{compr_str},", end="")

                # create the test_db
                test_db_path = f"{test_db_dir}/perf_test_{compr_str}_{str(time.time())}"
                opts = aimrocks.Options()
                opts.create_if_missing = True
                opts.error_if_exists = True
                opts.table_factory = aimrocks.BlockBasedTableFactory(
                    block_size=block_size
                )
                test_db = aimrocks.DB(test_db_path, opts, read_only=False)

                # for each in df, put in test_db
                index_len = len(str(len(df)))
                tot_put_size = 0
                tot_put_time = 0
                batch_write = aimrocks.WriteBatch()
                for i, row in df.iterrows():
                    index = str(i).zfill(index_len)
                    content = str(row["content"])
                    tot_put_size += len(content)
                    put_start = time.time()
                    batch_write.put(str.encode(index), str.encode(content))
                    put_end = time.time()
                    tot_put_time += put_end - put_start
                    if i % 65536 == 0:  # like iter_batches
                        put_start = time.time()
                        test_db.write(batch_write)
                        batch_write.clear()
                        put_end = time.time()
                        tot_put_time += put_end - put_start
                # write the remainings of the batch
                put_start = time.time()
                test_db.write(batch_write)
                put_end = time.time()
                tot_put_time += put_end - put_start
                tot_put_size_kb = round(tot_put_size / KiB)
                print(f"{tot_put_size_kb},{round(tot_put_time, 3)},", end="")
                print(f"{round(tot_put_time/len(df), 5)},", end="")
                print(f"{round(tot_put_size_kb/tot_put_time, 3)}")

                # delete the test_db
                del test_db
                if os.path.exists(test_db_path):
                    shutil.rmtree(test_db_path)

    print(f"\nEnding at {time.asctime()}", flush=True)
