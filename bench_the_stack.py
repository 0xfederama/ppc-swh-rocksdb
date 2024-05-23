#!/usr/bin/env python3

import os
import shutil
import time
import aimrocks
import pandas as pd
import pyarrow.parquet as pq
import numpy as np
import tlsh
import json

"""
The parquet file is the file for which the db_test is created (storing index and content).
The db_contents is already created, and it is static, with only sha and content.
"""
parq_size = "small_4096M"  # small_5rec, small_1M, small_8M, small_64M, small_256M, small_850M, small_4096M, small_10G, small_200G, dedup_v1
small_parq_path = "/disk2/federico/the-stack/the-stack-" + parq_size + ".parquet"
full_parq_path = "/disk2/data/the-stack/the-stack-" + parq_size + ".parquet"
parq_path = small_parq_path if parq_size != "dedup_v1" else full_parq_path
parq_size_b = round(os.stat(parq_path).st_size)

db_contents_path = "/disk2/federico/the-stack/contents_db-uncomp-4K_block"
tmp_db_path = "/disk2/federico/db/tmp/"

KiB = 1024
MiB = 1024 * 1024
GiB = 1024 * 1024 * 1024
PID = os.getpid()


def create_fingerprints(content: str, fingerprints: list[str]) -> dict[str, str]:
    # create a dict of fingerprints
    out = {}
    for lsh in fingerprints:
        fingerprint = "0"
        if len(content) > 2**20:
            # don't compute hash for strings bigger than 1 MiB (keep "0")
            out[lsh] = fingerprint
            continue
        match lsh:
            case "tlsh":
                if len(content) > 50:  # requested by tlsh algorithm
                    # first 8 bytes are metadata
                    fingerprint = tlsh.hash(str.encode(content))[8:]
            # case "min_hash":
            #     fingerprint = hash(content)
        out[lsh] = fingerprint
    return out


def make_key(order, index_len, max_size, i, row):
    key = ""
    sha = str(row["hexsha"])
    match order:
        case "parquet":
            index = str(i).zfill(index_len)
            key = index + "-" + sha
        case "filename":
            key = str(row["filename"])[::-1] + "-" + sha
        case "filename_repo":
            key = str(row["filename"])[::-1] + "_" + str(row["repo"]) + "-" + sha
        case "repo_filename":
            key = str(row["repo"]) + "_" + str(row["filename"])[::-1] + "-" + sha
        case "fingerprint":
            size_len = len(str(max_size))
            size = str(row["size"]).zfill(size_len)
            key = str(row[order][lsh]) + "_" + size + "-" + sha
    return key


def test(
    db_contents: aimrocks.DB,
    metainfo_df: pd.DataFrame,
    compr: aimrocks.CompressionType,
    level: int,
    order: str,
    block_size: int,
    lsh: str,
    max_size: int,
):
    ######################
    # create the test db #
    ######################
    db_test_path = (
        f"{tmp_db_path}db_{parq_size}_{str(compr)}_{str(block_size)}_{int(time.time())}"
    )
    opts = aimrocks.Options()
    opts.create_if_missing = True
    opts.error_if_exists = True
    opts.compression = compr
    if level != 0:
        opts.compression_opts = {"level": level}
    opts.table_factory = aimrocks.BlockBasedTableFactory(block_size=block_size)
    db_test = aimrocks.DB(db_test_path, opts, read_only=False)

    print_level = ""
    if compr != aimrocks.CompressionType.no_compression:
        print_level = "-" + str(level)
    print(f"{block_size/KiB},{opts.compression}{print_level},", end="")

    ##################
    # sort if needed #
    ##################
    sorting_time = 0
    sorted_df = metainfo_df
    if compr != aimrocks.CompressionType.no_compression and order != "parquet":
        start_sorting = time.time()
        match order:
            case "filename_repo":
                sorted_df = metainfo_df.sort_values(
                    by=["filename", "repo"],
                    key=lambda x: (
                        x
                        if x.name != "filename"
                        else x.map(lambda filename: filename[::-1])
                    ),
                    ignore_index=True,
                )
            case "repo_filename":
                sorted_df = metainfo_df.sort_values(
                    by=["repo", "filename"],
                    key=lambda x: (
                        x
                        if x.name != "filename"
                        else x.map(lambda filename: filename[::-1])
                    ),
                    ignore_index=True,
                )
            case "filename":
                sorted_df = metainfo_df.sort_values(
                    by=["filename"],
                    key=lambda x: (
                        x
                        if x.name != "filename"
                        else x.map(lambda filename: filename[::-1])
                    ),
                    ignore_index=True,
                )
            case "fingerprint":
                sorted_df = metainfo_df.sort_values(
                    by=["fingerprint", "size"],
                    key=lambda x: (
                        x
                        if x.name != "fingerprint"
                        else x.map(lambda fingerprint: fingerprint[lsh])
                    ),
                    ignore_index=True,
                    ascending=[True, False],
                )
        end_sorting = time.time()
        sorting_time = round(end_sorting - start_sorting, 3)
    print_lsh = ""
    if order == "fingerprint":
        print_lsh = "-" + lsh
    print(f"{order}{print_lsh},{sorting_time},", end="")

    #####################
    # build the test db #
    #####################
    start_insert = time.time()
    index_len = len(str(len(metainfo_df)))
    batch_size = 1000
    sha_queries = {}  #  dictionary sha: (i, row)
    # for each row in df, get from contents_db and insert in test_db
    for i, row in sorted_df.iterrows():
        sha = str.encode(str(row["hexsha"]))
        sha_queries[sha] = (i, row)
        if len(sha_queries) % batch_size == 0:
            queried = db_contents.multi_get(list(sha_queries.keys()))
            batch_write = aimrocks.WriteBatch()
            for sha, content in queried.items():
                i = sha_queries[sha][0]
                row = sha_queries[sha][1]
                key = make_key(order, index_len, max_size, i, row)
                batch_write.put(str.encode(key), content)
            db_test.write(batch_write)
            sha_queries.clear()
            batch_write.clear()
    # write the remainings of the batch
    if len(sha_queries) > 0:
        queried = db_contents.multi_get(list(sha_queries.keys()))
        batch_write = aimrocks.WriteBatch()
        for sha, content in queried.items():
            i = sha_queries[sha][0]
            row = sha_queries[sha][1]
            key = make_key(order, index_len, max_size, i, row)
            batch_write.put(str.encode(key), content)
        db_test.write(batch_write)
        sha_queries.clear()
        batch_write.clear()
    end_insert = time.time()
    insert_time = end_insert - start_insert
    avg_insert_time = round(insert_time / len(metainfo_df), 5)
    print(f"{avg_insert_time},", end="")

    #########################################
    # measure db size and compression ratio #
    #########################################
    total_db_size = 0
    for dirpath, _, filenames in os.walk(db_test_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if not os.path.islink(fp):
                total_db_size += os.path.getsize(fp)
    compression_ratio = round((total_db_size * 100) / parq_size_b, 3)
    total_db_size_gb = round(total_db_size / GiB, 3)
    print(f"{compression_ratio},{total_db_size_gb},", end="")

    ########################
    # measure access times #
    ########################
    n_queries = 500  # 0: query entire db, X: make X queries
    if n_queries == 0 or n_queries > len(metainfo_df):
        n_queries = len(metainfo_df)
    queries = list(np.random.permutation(len(metainfo_df))[:n_queries])
    query_log = []
    found_sg = 0
    found_mg10 = 0
    found_mg100 = 0
    got_size = 0
    ind_query = 1
    keys_get_10 = []
    keys_get_100 = []
    tot_sg_time = 0
    tot_mg10_time = 0
    tot_mg100_time = 0
    index_len = len(str(len(metainfo_df)))
    for i, row in metainfo_df.iterrows():
        if int(i) in queries:
            key = make_key(order, index_len, max_size, i, row)
            query_log.append(str(key))
            keys_get_10.append(str.encode(key))
            keys_get_100.append(str.encode(key))
            # test single get
            start_sg_time = time.time()
            got = db_test.get(str.encode(key))
            end_sg_time = time.time()
            tot_sg_time += end_sg_time - start_sg_time
            got_size += len(got)
            found_sg += sum(x is not None for x in [got])
            # test multi gets
            if ind_query % 10 == 0:
                start_mg_time = time.time()
                gotlist = db_test.multi_get(keys_get_10)
                end_mg_time = time.time()
                keys_get_10.clear()
                tot_mg10_time += end_mg_time - start_mg_time
                found_mg10 += sum(x is not None for x in gotlist)
            if ind_query % 100 == 0:
                start_mg_time = time.time()
                gotlist = db_test.multi_get(keys_get_100)
                end_mg_time = time.time()
                keys_get_100.clear()
                tot_mg100_time += end_mg_time - start_mg_time
                found_mg100 += sum(x is not None for x in gotlist)
            ind_query += 1
    if found_sg != len(queries):
        print(f"\nERROR: found {found_sg} out of {len(queries)} queries")
    if not (found_sg == found_mg10 == found_mg100):
        print(f"ERROR: found numbers differ: {found_sg}, {found_mg10}, {found_mg100}")
    # compute times
    avg_sg_time = tot_sg_time / n_queries
    avg_mg10_time = tot_mg10_time / n_queries
    avg_mg100_time = tot_mg100_time / n_queries
    sg_throughput = (got_size / MiB) / tot_sg_time
    mg10_throughput = (got_size / MiB) / tot_mg10_time
    mg100_throughput = (got_size / MiB) / tot_mg100_time
    print(
        f"{round(avg_sg_time * 1000, 3)},{round(sg_throughput, 3)},{round(avg_mg10_time * 1000, 3)},{round(mg10_throughput, 3)},{round(avg_mg100_time * 1000, 3)},{round(mg100_throughput, 3)}"
    )
    # print the query log to file
    with open(
        f"query_log-{PID}/{str(compr)}_{block_size}_{order}_{lsh}.json", "w"
    ) as f:
        f.write(json.dumps(query_log, indent=4))

    #################
    # delete the db #
    #################
    del db_test
    if os.path.exists(db_test_path):
        shutil.rmtree(db_test_path)


if __name__ == "__main__":
    print(f"Start computation at {time.asctime()}")
    print(f"PID: {PID}")
    print(f"User: {os.getlogin()}")
    print(f"Hostname: {os.uname()[1]}")
    print(f"Contents RocksDB in {db_contents_path}")
    print(f"Putting temp RocksDBs in {tmp_db_path}")
    print(f"Dataset {parq_path}, size {round(parq_size_b / MiB, 3)} MiB")
    print()

    # declare different tests
    compressions = [
        (aimrocks.CompressionType.no_compression, 0),
        (aimrocks.CompressionType.zstd_compression, 3),
        # (aimrocks.CompressionType.zstd_compression, 12),
        # (aimrocks.CompressionType.zstd_compression, 22),
        (aimrocks.CompressionType.snappy_compression, 0),
        (aimrocks.CompressionType.zlib_compression, 0),
    ]
    block_sizes = [
        4 * KiB,
        256 * KiB,
        # 512 * KiB,
        # 1 * MiB,
        # 4 * MiB,
    ]
    orders = [
        "parquet",  # standard order of the parquet file (by language)
        "filename",
        "filename_repo",
        "repo_filename",
        "fingerprint",
    ]
    fingerprints = [
        "tlsh",
        # "min_hash",
    ]

    # open the contents db
    opts = aimrocks.Options()
    opts.create_if_missing = False
    opts.max_open_files = 100000
    db_contents = aimrocks.DB(db_contents_path, opts, read_only=True)

    # read parquet to create metadata db (dataframe)
    dataframes = []
    parquet_file = pq.ParquetFile(parq_path)
    for batch in parquet_file.iter_batches(
        columns=[
            "hexsha",
            "max_stars_repo_path",
            "max_stars_repo_name",
            "content",
            "size",
        ]
    ):
        batch_df = batch.to_pandas()
        # replace content with its fingerprint
        batch_df["content"] = batch_df["content"].apply(
            create_fingerprints, fingerprints=fingerprints
        )
        dataframes.append(batch_df)
    # concatenate the results and rename columns
    metainfo_df = pd.concat(dataframes, ignore_index=True)
    metainfo_df = metainfo_df.rename(
        columns={
            "max_stars_repo_path": "filename",
            "max_stars_repo_name": "repo",
            "content": "fingerprint",
        }
    )
    max_size = metainfo_df["size"].max()

    # print header
    print(
        "BLOCK_SIZE(KiB),COMPRESSION,ORDERING,SORTING_TIME(s),AVG_INSERT_TIME(s),COMPRESSION_RATIO(%),TOT_SIZE(GiB),AVG_SINGLE_GET_TIME(ms),SINGLE_GET_THROUGHPUT(KiB/s),AVG_MULTI_GET_10_TIME(ms),MULTI_GET_10_THROUGHPUT(MiB/S),AVG_MULTI_GET_100_TIME(ms),MULTI_GET_100_THROUGHPUT(MiB/S)"
    )

    # create query log directory
    os.makedirs(f"query_log-{PID}")

    for block_size in block_sizes:
        for compr in compressions:
            test_orders = orders
            test_fingerprints = ["no_lsh"]
            if compr[0] == aimrocks.CompressionType.no_compression:
                # without compression the order and the lsh are useless
                test_orders = ["parquet"]
            for order in test_orders:
                if order == "fingerprint":
                    test_fingerprints = fingerprints
                for lsh in test_fingerprints:
                    test(
                        db_contents=db_contents,
                        metainfo_df=metainfo_df,
                        compr=compr[0],
                        level=compr[1],
                        order=order,
                        block_size=block_size,
                        lsh=lsh,
                        max_size=max_size,
                    )

    print(f"\nEnd computation at {time.asctime()}")
