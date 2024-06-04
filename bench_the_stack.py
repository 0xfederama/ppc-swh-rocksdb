#!/usr/bin/env python3

import json
import mmap
import os
import shutil
import time

import aimrocks
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import tlsh

querylog = False

"""
The parquet file is the file for which the db_test is created (storing index and content).
The db_contents is already created, and it is static, with only sha and content.
"""
parq_size = "1G"  # 5rec, 1M, 8M, 64M, 256M, 1G, 4G, 10G, 200G, dedup_v1, 1G_minsize_4M, 2G_minsize_1M, 10G_minsize_1012K, 24G_minsize_990K
small_parq_path = "/disk2/federico/the-stack/the-stack-" + parq_size + ".parquet"
full_parq_path = "/disk2/data/the-stack/the-stack-" + parq_size + ".parquet"
parq_path = small_parq_path if parq_size != "dedup_v1" else full_parq_path
parq_size_b = round(os.stat(parq_path).st_size)

txt_contents_path = "/disk2/federico/the-stack/the-stack-dedup_v1.txt"
txt_index_path = "/disk2/federico/the-stack/the-stack-dedup_v1-index.json"
tmp_db_path = "/disk2/federico/db/tmp/"

KiB = 1024
MiB = 1024 * 1024
GiB = 1024 * 1024 * 1024
PID = os.getpid()

metrics = ["compr_ratio", "ins_thr", "sg_thr", "mg_thr"]
results = {}


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


def get_compr_str(compr: tuple[aimrocks.CompressionType, int]):
    c_str = compr[0]
    if compr[0] != aimrocks.CompressionType.no_compression and compr[1] != 0:
        c_str += "-" + str(compr[1])
    return c_str.replace("_compression", "")


def get_bs_str(bs: int):
    return str(round(bs / KiB)) + " KiB"


def test(
    txt_mmap: mmap,
    txt_index: dict[str, dict],
    metainfo_df: pd.DataFrame,
    compressor: tuple[aimrocks.CompressionType, int],
    order: str,
    block_size: int,
    lsh: str,
    max_size: int,
):
    ######################
    # create the test db #
    ######################
    compr = compressor[0]
    level = compressor[1]
    db_test_path = (
        f"{tmp_db_path}db_{parq_size}_{str(compr)}_{str(block_size)}_{int(time.time())}"
    )
    opts = aimrocks.Options()
    opts.create_if_missing = True
    opts.error_if_exists = True
    # options to make db faster
    opts.allow_mmap_reads = True
    opts.paranoid_checks = False
    opts.use_adaptive_mutex = True
    opts.compression = compr
    if level != 0:
        opts.compression_opts = {"level": level}
    opts.table_factory = aimrocks.BlockBasedTableFactory(block_size=block_size)
    db_test = aimrocks.DB(db_test_path, opts, read_only=False)

    compr_str = get_compr_str(compressor)
    bs_str = get_bs_str(block_size)
    print(f"{block_size/KiB},{compr_str},", end="")

    ##################
    # sort if needed #
    ##################
    sorted_df = metainfo_df
    if compr != aimrocks.CompressionType.no_compression and order != "parquet":
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
    print_lsh = ""
    if order == "fingerprint":
        print_lsh = "-" + lsh
    print(f"{order}{print_lsh},", end="")

    #####################
    # build the test db #
    #####################
    start_insert = time.time()
    index_len = len(str(len(metainfo_df)))
    batch_size = 10000
    ins_size = 0
    # for each row in df, get from txt_contents and insert in test_db
    batch_write = aimrocks.WriteBatch()
    for i, row in sorted_df.iterrows():
        sha = str(row["hexsha"])
        ins_size += int(str(row["size"]))
        coords = txt_index[sha]
        start = coords[0]
        length = coords[1]
        txt_mmap.seek(start)
        content = txt_mmap.read(length)
        key = make_key(order, index_len, max_size, i, row)
        batch_write.put(str.encode(key), str.encode(content))
        if int(i) % batch_size == 0:
            db_test.write(batch_write)
            batch_write.clear()
    if batch_write.count() > 0:
        db_test.write(batch_write)
        batch_write.clear()
    end_insert = time.time()
    tot_insert_time = end_insert - start_insert
    ins_throughput = round(ins_size / KiB / tot_insert_time, 3)
    results["ins_thr"][bs_str][compr_str] = ins_throughput
    print(f"{ins_throughput},", end="")

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
    results["compr_ratio"][bs_str][compr_str] = compression_ratio
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
    found_mg = 0
    got_size = 0
    ind_query = 1
    keys_mget = []
    tot_sg_time = 0
    tot_mg_time = 0
    index_len = len(str(len(metainfo_df)))
    for i, row in metainfo_df.iterrows():
        if int(i) in queries:
            key = make_key(order, index_len, max_size, i, row)
            query_log.append(str(key))
            keys_mget.append(str.encode(key))
            # test single get
            start_sg_time = time.time()
            got = db_test.get(str.encode(key))
            end_sg_time = time.time()
            tot_sg_time += end_sg_time - start_sg_time
            got_size += len(got)
            found_sg += sum(x is not None for x in [got])
            # test multi get
            if ind_query % 100 == 0:
                start_mg_time = time.time()
                gotlist = db_test.multi_get(keys_mget)
                end_mg_time = time.time()
                keys_mget.clear()
                tot_mg_time += end_mg_time - start_mg_time
                found_mg += sum(x is not None for x in gotlist)
            ind_query += 1
    # test remainings of multiget
    if len(keys_mget) > 0:
        start_mg_time = time.time()
        gotlist = db_test.multi_get(keys_mget)
        end_mg_time = time.time()
        keys_mget.clear()
        tot_mg_time += end_mg_time - start_mg_time
        found_mg += sum(x is not None for x in gotlist)
    if found_sg != len(queries):
        print(f"\nERROR: found {found_sg} out of {len(queries)} queries")
    if not (found_sg == found_mg):
        print(f"ERROR: found numbers differ: {found_sg}, {found_mg}")
    # compute times
    sg_thr = (got_size / MiB) / tot_sg_time
    mg_thr = (got_size / MiB) / tot_mg_time
    results["sg_thr"][bs_str][compr_str] = round(sg_thr, 3)
    results["mg_thr"][bs_str][compr_str] = round(mg_thr, 3)
    print(f"{round(sg_thr, 3)},{round(mg_thr, 3)}")
    # print the query log to file
    if querylog:
        with open(f"query_log-{PID}/{compr_str}_{bs_str}_{order}_{lsh}.json", "w") as f:
            f.write(json.dumps(query_log, indent=4))

    #################
    # delete the db #
    #################
    del db_test
    del sorted_df
    if os.path.exists(db_test_path):
        shutil.rmtree(db_test_path)


if __name__ == "__main__":
    print(f"Start computation at {time.asctime()}")
    print(f"PID: {PID}")
    print(f"User: {os.getlogin()}")
    print(f"Hostname: {os.uname()[1]}")
    print(f"Content txt in {txt_contents_path}")
    print(f"Putting temp RocksDBs in {tmp_db_path}")
    print(f"Dataset {parq_path}, size {round(parq_size_b / MiB, 3)} MiB")
    print()

    # declare different tests
    compressors = [
        (aimrocks.CompressionType.no_compression, 0),
        (aimrocks.CompressionType.zstd_compression, 3),
        (aimrocks.CompressionType.zstd_compression, 12),
        (aimrocks.CompressionType.zstd_compression, 22),
        (aimrocks.CompressionType.zlib_compression, 6),
        (aimrocks.CompressionType.zlib_compression, 9),
        (aimrocks.CompressionType.snappy_compression, 0),
    ]
    block_sizes = [
        4 * KiB,
        8 * KiB,
        64 * KiB,
        128 * KiB,
        256 * KiB,
        # 512 * KiB,
        # 1 * MiB,
        # 4 * MiB,
        # 10 * MiB,
    ]
    orders = [
        # "parquet",  # standard order of the parquet file (by language)
        "filename",
        # "filename_repo",
        # "repo_filename",
        # "fingerprint",
    ]
    fingerprints = [
        "tlsh",
        # "min_hash",
    ]

    # open the contents txt with mmap and the index file
    txt_contents_file = open(txt_contents_path, "r")
    txt_mmap = mmap.mmap(txt_contents_file.fileno(), length=0, access=mmap.PROT_READ)
    with open(txt_index_path, "r") as f:
        txt_index = json.load(f)

    # read parquet to create metadata db (dataframe)
    start_reading = time.time()
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
    end_reading = time.time()
    print(
        f"Reading parquet and computing fingerprints: {round(end_reading - start_reading)} s\n"
    )

    # print header
    print(
        "BLOCK_SIZE(KiB),COMPRESSION,ORDER,INSERT_THROUGHPUT(KiB/s),COMPRESSION_RATIO(%),TOT_SIZE(GiB),SINGLE_GET_THROUGHPUT(MiB/s),MULTI_GET_THROUGHPUT(MiB/S)"
    )

    # create query log directory
    if querylog:
        os.makedirs(f"query_log-{PID}")

    # setup histogram results dictionary
    for m in metrics:
        results[m] = {}
        for b in block_sizes:
            bs_str = get_bs_str(b)
            if bs_str not in results[m]:
                results[m][bs_str] = {}
            for c in compressors:
                c_str = get_compr_str(c)
                results[m][bs_str][c_str] = 0
    x_compr = list(next(iter(results["compr_ratio"].values())).keys())
    x_blocksizes = list(results["compr_ratio"].keys())

    for block_size in block_sizes:
        for compr in compressors:
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
                        txt_mmap=txt_mmap,
                        txt_index=txt_index,
                        metainfo_df=metainfo_df,
                        compressor=compr,
                        order=order,
                        block_size=block_size,
                        lsh=lsh,
                        max_size=max_size,
                    )
    print()

    # create histograms for the results
    charts_dir = f"charts_benchmark-{PID}"
    os.makedirs(charts_dir)
    for m in metrics:
        x = np.arange(len(x_compr))
        width = 0.15
        multiplier = -1
        data = results[m]
        fig, ax = plt.subplots(figsize=(11, 6))
        stripped_results = {key: tuple(value.values()) for key, value in data.items()}
        for size, value in stripped_results.items():
            offset = width * multiplier
            barlabel = ax.bar(x + offset, value, width, label=size)
            # ax.bar_label(barlabel, padding=3)
            multiplier += 1
        ax.set_xlabel("Compressors")
        match m:
            case "compr_ratio":
                ax.set_ylabel("Compression ratio (%)")
            case "ins_thr":
                ax.set_ylabel("Insertion throughput (MiB/s)")
            case "sg_thr":
                ax.set_ylabel("Single get throughput (MiB/s)")
            case "mg_thr":
                ax.set_ylabel("Multi get throughput (MiB/s)")
        ax.set_xticks(x + width, x_compr)
        ax.legend(title="Block sizes", alignment="left", loc="upper left")
        plt.savefig(f"{charts_dir}/{m}.png", format="png", bbox_inches="tight", dpi=120)
        plt.close()
        print(f"Graph {m} created")

    print(f"\nEnd computation at {time.asctime()}")
