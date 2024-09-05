import json
import os
import shutil
import time

import aimrocks
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import tlsh

querylog = False  # True to output queries to file, False to skip it
make_charts = True  # True to create charts, False to skip it
delete_db = True  # True to delete the test dbs, False to skip it
n_queries = 500  # number of queries to make on the dbs to test their throughput

parq_size = "10G"  # 5rec, 1M, 8M, 64M, 256M, 1G, 4G, 10G, 200G, dedup_v1, 1G_minsize_4M, 2G_minsize_1M, 10G_minsize_1012K, 24G_minsize_990K
small_parq_path = "/weka1/federico/the-stack/small/the-stack-" + parq_size + ".parquet"
full_parq_path = "/weka1/federico/the-stack/the-stack-" + parq_size + "-zstd.parquet"
parq_path = small_parq_path if "dedup_v1" not in parq_size else full_parq_path
# parq_path = "/weka1/federico/the-stack/langs/the-stack-60G-py.parquet"
parq_size_b = os.path.getsize(parq_path)

tmp_test_path = "/weka1/federico/db/tmp/"

KiB = 1024
MiB = 1024 * 1024
GiB = 1024 * 1024 * 1024
PID = os.getpid()

metrics = ["compr_ratio", "ins_thr", "sg_thr", "mg_thr"]
results = {}


def compute_fingerprint(content: str, fingerprint: str):
    lsh = "0"
    # don't compute hash for strings bigger than 1 MiB (keep "0")
    if len(content) > 1 * MiB:
        return lsh
    match fingerprint:
        case "tlsh":
            if len(content) > 50:  # requested by tlsh algorithm
                # first 8 bytes are metadata
                try:
                    lsh = tlsh.hash(str.encode(content))[8:]
                except Exception as e:
                    print(f"ERROR IN CREATE_FINGERPRINTS: {e}")
        # case "min_hash":
        #     fingerprint = hash(content)
    return lsh


def make_key(order, index_len, max_size, i, row, lsh):
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

def reverse_filename_tosoni(input_path: str):
    # implement reversed string
    # given path/to/file.cpp, return cpp.file/ot/htap
    if "/" in input_path:
        path, filename_with_extension = input_path.rsplit("/", 1)
    else:
        path, filename_with_extension = "", input_path
    filename, extension = filename_with_extension.rsplit(".", 1)
    reversed_path = path[::-1] if path else ""
    if reversed_path:
        transformed_path = f"{extension}.{filename}/{reversed_path}"
    else:
        transformed_path = f"{extension}.{filename}"

    return transformed_path

def get_compr_str(compr: tuple[aimrocks.CompressionType, int]):
    c_str = compr[0]
    if compr[0] != aimrocks.CompressionType.no_compression and compr[1] != 0:
        c_str += "-" + str(compr[1])
    return c_str.replace("_compression", "")


def get_bs_str(bs: int):
    return str(round(bs / KiB)) + " KiB"


def sort_df(input_df: pd.DataFrame, order: str, lsh: str):
    sorted_df = input_df
    if order != "parquet":
        match order:
            case "filename_repo":
                sorted_df = input_df.sort_values(
                    by=["filename", "repo"],
                    key=lambda x: (
                        x
                        if x.name != "filename"
                        else x.map(lambda filename: filename[::-1])
                    ),
                    ignore_index=True,
                )
            case "repo_filename":
                sorted_df = input_df.sort_values(
                    by=["repo", "filename"],
                    key=lambda x: (
                        x
                        if x.name != "filename"
                        else x.map(lambda filename: filename[::-1])
                    ),
                    ignore_index=True,
                )
            case "filename":
                sorted_df = input_df.sort_values(
                    by=["filename"],
                    key=lambda x: (
                        x
                        if x.name != "filename"
                        else x.map(lambda filename: filename[::-1])
                    ),
                    ignore_index=True,
                )
            case "fingerprint":
                sorted_df = input_df.sort_values(
                    by=["fingerprint", "size"],
                    key=lambda x: (
                        x
                        if x.name != "fingerprint"
                        else x.map(lambda fingerprint: fingerprint[lsh])
                    ),
                    ignore_index=True,
                    ascending=[True, False],
                )
    return sorted_df


def test_rocksdb(
    compressor: tuple[aimrocks.CompressionType, int],
    order: str,
    block_size: int,
    lsh: str,
    table_len: int,
    index_len: int,
    max_size: int,
    queries: list[int],
):
    ######################
    # create the test db #
    ######################
    compr = compressor[0]
    level = compressor[1]
    db_test_path = f"{tmp_test_path}db_{parq_size}_{str(compr)}_{str(block_size)}_{int(time.time())}"
    opts = aimrocks.Options()
    opts.create_if_missing = True
    opts.error_if_exists = True
    # options to make db faster
    opts.allow_mmap_reads = True
    opts.paranoid_checks = False
    opts.use_adaptive_mutex = True
    # options to try to make db smaller
    # opts.compression_opts["max_dict_bytes"] = 1 * GiB
    # compression and block
    opts.compression = compr
    if level != 0:
        opts.compression_opts = {"level": level}
        # opts.compression_opts["level"] = level
    opts.table_factory = aimrocks.BlockBasedTableFactory(block_size=block_size)
    db_test = aimrocks.DB(db_test_path, opts, read_only=False)

    compr_str = get_compr_str(compressor)
    bs_str = get_bs_str(block_size)
    print_lsh = ""
    if order == "fingerprint":
        print_lsh = "-" + lsh
    print(f"{block_size/KiB},{compr_str},{order}{print_lsh},", end="")

    #####################
    # build the test db #
    #####################
    tot_insert_time = 0
    ins_size = 0
    ind_parq = 0
    query_log = []
    parquet_file = pq.ParquetFile(parq_path)
    batch_write = aimrocks.WriteBatch()
    for batch in parquet_file.iter_batches():
        for i in range(len((batch))):
            sha = str(batch["hexsha"][i])
            content = str(batch["content"][i])
            cont_size = int(str(batch["size"][i]))
            filename = str(batch["max_stars_repo_path"][i])
            repo = str(batch["max_stars_repo_name"][i])
            ins_size += cont_size
            key = ""
            match order:
                case "parquet":
                    index = str(i).zfill(index_len)
                    key = index + "-" + sha
                case "filename":
                    key = filename[::-1] + "-" + sha
                case "filename_tosoni":
                    key = reverse_filename_tosoni(filename) + "-" + sha
                case "filename_repo":
                    key = filename[::-1] + "_" + repo + "-" + sha
                case "repo_filename":
                    key = repo + "_" + filename[::-1] + "-" + sha
                case "fingerprint":
                    size_len = len(str(max_size))
                    size = str(cont_size).zfill(size_len)
                    fingerprint = compute_fingerprint(content, lsh)
                    key = str(fingerprint) + "_" + size + "-" + sha
            if ind_parq in queries:
                query_log.append(key)
            batch_write.put(str.encode(key), str.encode(content))
            ind_parq += 1
        start_write = time.time()
        db_test.write(batch_write)
        end_write = time.time()
        tot_insert_time += end_write - start_write
        batch_write.clear()
    # compute throughput
    ins_thr = round(ins_size / MiB / tot_insert_time, 2)
    results["ins_thr"][bs_str][compr_str] = ins_thr
    print(f"{ins_thr},", end="")

    #########################################
    # measure db size and compression ratio #
    #########################################
    tot_db_size = 0
    tot_sst_size = 0
    tot_sst_files = 0
    for dirpath, _, filenames in os.walk(db_test_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            try:
                if not os.path.islink(fp):
                    fsize = os.path.getsize(fp)
                    tot_db_size += fsize
                    if f.endswith(".sst"):
                        tot_sst_size += fsize
                        tot_sst_files += 1
            except Exception as e:
                print(e)
    compr_ratio = round((tot_db_size * 100) / parq_size_b, 2)
    avg_sst_size_mb = (
        round((tot_sst_size / MiB) / tot_sst_files, 2) if tot_sst_files != 0 else 0
    )
    results["compr_ratio"][bs_str][compr_str] = compr_ratio
    print(f"{compr_ratio},{avg_sst_size_mb},", end="")

    ### TEST IF DB IS IN ORDER
    ordered = True
    it = db_test.iterkeys()
    it.seek_to_first()
    it = list(it)
    for ind_iter in range(100):
        if not (it[ind_iter] <= it[ind_iter + 1]):
            print(f"\nERROR: {it[ind_iter]} not smaller than {it[ind_iter + 1]}")
            ordered = False
    print(f"{ordered},", end="")

    ########################
    # measure access times #
    ########################
    found_sg = 0
    found_mg = 0
    got_size = 0
    ind_query = 0
    keys_mget = []
    tot_sg_time = 0
    tot_mg_time = 0
    n_mget = 0
    for i, key in enumerate(query_log):
        keys_mget.append(str.encode(key))
        # test single get
        start_sg_time = time.time()
        got = db_test.get(str.encode(key))
        end_sg_time = time.time()
        tot_sg_time += end_sg_time - start_sg_time
        got_size += len(got)
        found_sg += sum(x is not None for x in [got])
        ind_query += 1
        # test multi get
        if ind_query % 100 == 0 or (i == table_len - 1 and len(keys_mget) > 0):
            start_mg_time = time.time()
            gotlist = db_test.multi_get(keys_mget)
            end_mg_time = time.time()
            keys_mget.clear()
            n_mget += 1
            tot_mg_time += end_mg_time - start_mg_time
            found_mg += sum(x is not None for x in gotlist)
    if found_sg != len(queries):
        print(f"\nERROR: found {found_sg} out of {len(queries)} queries")
    if not (found_sg == found_mg):
        print(f"\nERROR: found numbers differ: {found_sg}, {found_mg}")
    # compute times
    sg_thr = (got_size / MiB) / tot_sg_time
    mg_thr = (got_size / MiB) / tot_mg_time
    results["sg_thr"][bs_str][compr_str] = round(sg_thr, 2)
    results["mg_thr"][bs_str][compr_str] = round(mg_thr, 2)
    print(f"{round(sg_thr, 2)},{round(mg_thr, 2)},{n_mget} #mget")
    # print the query log to file
    if querylog:
        with open(
            f"query_log-{parq_size}-{PID}/{compr_str}_{bs_str}_{order}_{lsh}.json", "w"
        ) as f:
            f.write(json.dumps(query_log, indent=4))

    #################
    # delete the db #
    #################
    del db_test
    if delete_db:
        if os.path.exists(db_test_path):
            shutil.rmtree(db_test_path)


if __name__ == "__main__":
    print(f"Start computation at {time.asctime()}")
    print(f"PID: {PID}")
    print(f"User: {os.getlogin()}")
    print(f"Hostname: {os.uname()[1]}")
    print(f"Putting temp RocksDBs in {tmp_test_path}")
    print(f"Dataset {parq_path}, size {round(parq_size_b / MiB, 3)} MiB")
    print()

    # declare different tests
    orders = [
        # "parquet",  # standard order of the parquet file (by language)
        "filename",
        # "filename_tosoni",
        # "filename_repo",
        # "repo_filename",
        # "fingerprint",
    ]
    fingerprints = [
        "tlsh",
        # "min_hash",
    ]
    # define compressors and block sizes
    compressors = [
        (aimrocks.CompressionType.no_compression, 0),
        (aimrocks.CompressionType.zstd_compression, 3),
        # (aimrocks.CompressionType.zstd_compression, 12),
        # (aimrocks.CompressionType.zstd_compression, 22),
        (aimrocks.CompressionType.zlib_compression, 6),
        # (aimrocks.CompressionType.zlib_compression, 9),
        (aimrocks.CompressionType.snappy_compression, 0),
    ]
    block_sizes = [
        4 * KiB,
        # 8 * KiB,
        # 16 * KiB,
        # 32 * KiB,
        # 64 * KiB,
        # 128 * KiB,
        256 * KiB,
        # 512 * KiB,
        # 1 * MiB,
        4 * MiB,
        # 10 * MiB,
    ]

    print(f"Orderings: {orders}, fingerprints: {fingerprints}")
    print(f"Compressors: {[get_compr_str(c) for c in compressors]}")
    print(f"Block sizes: {[get_bs_str(b) for b in block_sizes]}")
    print()

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

    # get max_size and max_index
    table = pq.read_table(parq_path, columns=["size"])
    table_len = table.num_rows
    index_len = len(str(table_len))
    max_size = 0
    for i in table["size"]:
        size = i.as_py()
        if size > max_size:
            max_size = size

    # create query log directory
    if querylog:
        os.makedirs(f"query_log-{parq_size}-{PID}")

    # create queries list
    if n_queries == 0 or n_queries > table_len:
        n_queries = table_len
    queries = list(np.random.permutation(table_len)[:n_queries])

    print(
        "BLOCK_SIZE(KiB),COMPRESSION,ORDER,INSERT_THROUGHPUT(MiB/s),COMPRESSION_RATIO(%),AVG_SST_FILE_SIZE(MiB),DB_ORDERED,SINGLE_GET_THROUGHPUT(MiB/s),MULTI_GET_THROUGHPUT(MiB/S)"
    )
    # run tests
    for block_size in block_sizes:
        for compr in compressors:
            test_orders = orders
            test_fingerprints = ["no_lsh"]
            # if compr[0] == aimrocks.CompressionType.no_compression:
            #     # without compression the order and the lsh are useless
            #     test_orders = ["parquet"]
            for order in test_orders:
                if order == "fingerprint":
                    test_fingerprints = fingerprints
                for lsh in test_fingerprints:
                    test_rocksdb(
                        compressor=compr,
                        order=order,
                        block_size=block_size,
                        lsh=lsh,
                        table_len=table_len,
                        index_len=index_len,
                        max_size=max_size,
                        queries=queries,
                    )
    print()

    # create histograms for the results
    if make_charts:
        charts_dir = f"charts_benchmark-{parq_size}-{PID}"
        os.makedirs(charts_dir)
        for m in metrics:
            x = np.arange(len(x_compr))
            width = 0.15
            multiplier = -0.5
            data = results[m]
            fig, ax = plt.subplots(figsize=(9, 6))
            stripped_results = {
                key: tuple(value.values()) for key, value in data.items()
            }
            for size, value in stripped_results.items():
                offset = width * multiplier
                barlabel = ax.bar(x + offset, value, width, label=size)
                # ax.bar_label(barlabel, padding=3)
                multiplier += 1
            ax.set_xlabel("Compressor")
            legend_loc = "upper left"
            match m:
                case "compr_ratio":
                    ax.set_ylabel("Compression ratio (%)")
                    plt.yticks(list(plt.yticks()[0]) + [100])  # TODO: check if it works
                    legend_loc = "upper right"
                case "ins_thr":
                    ax.set_ylabel("Insertion throughput (MiB/s)")
                case "sg_thr":
                    ax.set_ylabel("Single get throughput (MiB/s)")
                case "mg_thr":
                    ax.set_ylabel("Multi get throughput (MiB/s)")
            ax.set_xticks(x + width, x_compr)
            ax.legend(title="Block sizes", alignment="left", loc=legend_loc)
            plt.savefig(
                f"{charts_dir}/{m}.png", format="png", bbox_inches="tight", dpi=120
            )
            plt.close()
            print(f"Graph {m} created")
        print()

    print(f"End computation at {time.asctime()}")
