import json
import os
import shutil
import time

import aimrocks
import matplotlib.pyplot as plt
import numpy as np
import pyarrow.parquet as pq
import tlsh

import config

querylog = False  # True to output queries to file, False to skip it
make_charts = False  # True to create charts, False to skip it
keep_db = True  # True to delete the test dbs, False to skip it
readonly = False  # True to close db and reopen in readonly, False to skip it
n_queries = 10000  # number of queries to make on the dbs to test their throughput

parq_size_b = os.path.getsize(config.parquet_path)

KiB = 1024
MiB = 1024 * 1024
GiB = 1024 * 1024 * 1024
PID = os.getpid()

metrics = ["compr_ratio", "ins_thr", "sg_thr", "mg_thr"]
results = {}


def create_tlsh(content) -> str:
    if type(content) != str:
        content = content.decode("latin-1")
    fingerprint = "0"
    # don't compute hash for strings bigger than 1 MiB (keep "0")
    if len(content) > 1 * MiB:
        return fingerprint
    if len(content) > 50:  # requested by tlsh algorithm
        # first 8 bytes are metadata
        try:
            fingerprint = tlsh.hash(str.encode(content))[8:]
        except Exception as e:
            print(f"ERROR IN CREATE_FINGERPRINTS: {e}")
    return fingerprint


def make_key(order, index_len, max_size, i, row):
    key = ""
    sha = str(row["hexsha"])
    match order:
        case "parquet":
            index = str(i).zfill(index_len)
            key = index + "-" + sha
        case "rev-filename":
            size_len = len(str(max_size))
            size = str(row["size"]).zfill(size_len)
            filename = str(row["filename"])
            if filename is None:
                filename = ""
            key = filename[::-1] + "_" + size + "-" + sha
        case "ext-filename":
            size_len = len(str(max_size))
            size = str(row["size"]).zfill(size_len)
            filename = str(row["filename"])
            if filename is None:
                filename = ""
            key = reverse_filename_tosoni(filename) + "_" + size + "-" + sha
        case "ext-filename-nopath":
            size_len = len(str(max_size))
            size = str(row["size"]).zfill(size_len)
            filename = str(row["filename"])
            if filename is None:
                filename = ""
            key = reverse_filename_tosoni_nopath(filename) + "_" + size + "-" + sha
        case "lang-ext-filename":
            key = (
                str(row["lang"])
                + "-"
                + reverse_filename_tosoni(str(row["filename"]))
                + "-"
                + sha
            )
        case "filename_repo":
            key = str(row["filename"])[::-1] + "_" + str(row["repo"]) + "-" + sha
        case "repo_filename":
            key = str(row["repo"]) + "_" + str(row["filename"])[::-1] + "-" + sha
        case "tlsh":
            size_len = len(str(max_size))
            size = str(row["size"]).zfill(size_len)
            key = str(row["tlsh"]) + "_" + size + "-" + sha
    return key


def reverse_filename_tosoni(input_path: str):
    if input_path is None:
        return ""
    # implement reversed string
    # given path/to/file.cpp, return cpp.file/ot/htap
    if "/" in input_path:
        path, filename_with_extension = input_path.rsplit("/", 1)
    else:
        path, filename_with_extension = "", input_path
    transformed_name = filename_with_extension
    if "." in transformed_name:
        filename, extension = filename_with_extension.rsplit(".", 1)
        transformed_name = f"{extension}.{filename}"
    reversed_path = path[::-1] if path else ""
    if reversed_path:
        transformed_path = f"{transformed_name}/{reversed_path}"
    else:
        transformed_path = f"{transformed_name}"

    return transformed_path


def reverse_filename_tosoni_nopath(input_path: str):
    if input_path is None:
        return ""
    filename_with_extension = input_path.split("/")[-1]
    transformed_name = filename_with_extension
    if "." in transformed_name:
        filename, extension = filename_with_extension.rsplit(".", 1)
        transformed_name = f"{extension}.{filename}"
    return transformed_name


def get_compr_str(compr: tuple[aimrocks.CompressionType, int]):
    c_str = compr[0]
    if compr[0] != aimrocks.CompressionType.no_compression and compr[1] != 0:
        c_str += "-" + str(compr[1])
    return c_str.replace("_compression", "")


def get_bs_str(bs: int):
    return str(round(bs / KiB)) + " KiB"


def test_rocksdb(
    compressor: tuple[aimrocks.CompressionType, int],
    order: str,
    block_size: int,
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
    db_test_path = f"{config.rocksdb_output_path}db_{str(compr)}_{str(block_size)}_{int(time.time())}"
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
    print(f"{block_size/KiB},{compr_str},{order},", end="")

    #####################
    # build the test db #
    #####################
    tot_insert_time = 0
    ins_size = 0
    index_parq = 0
    query_log = []
    parquet_file = pq.ParquetFile(config.parquet_path)
    batch_write = aimrocks.WriteBatch()
    for batch in parquet_file.iter_batches(
        columns=[
            "hexsha",
            "max_stars_repo_path",
            # "max_stars_repo_name",
            "content",
            "size",
            "lang",
        ]
    ):
        batch = batch.rename_columns(
            {
                "hexsha": "hexsha",
                "filename": "max_stars_repo_path",
                # "repo": "max_stars_repo_name",
                "tlsh": "content",
                "size": "size",
                "lang": "lang",
            }
        )
        batch_list = batch.to_pylist()
        for row in batch_list:
            content = str(row["tlsh"])
            cont_size = int(str(row["size"]))
            row["tlsh"] = create_tlsh(content)
            ins_size += cont_size
            key = make_key(order, index_len, max_size, index_parq, row)
            if index_parq in queries:
                query_log.append(key)
            index_parq += 1
            batch_write.put(str.encode(key), str.encode(content))
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

    ### Close the DB and reopen it
    if readonly:
        db_test.close()
        opts = aimrocks.Options()
        opts.create_if_missing = False
        opts.error_if_exists = False
        opts.allow_mmap_reads = True
        opts.paranoid_checks = False
        opts.use_adaptive_mutex = True
        opts.compression = compr
        if level != 0:
            opts.compression_opts = {"level": level}
        opts.table_factory = aimrocks.BlockBasedTableFactory(block_size=block_size)
        db_test_read = aimrocks.DB(db_test_path, opts, read_only=True)
    else:
        db_test_read = db_test

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
    print(f"{round(sg_thr, 2)},{round(mg_thr, 2)}")
    # print the query log to file
    if querylog:
        with open(
            f"query_log-{PID}/{compr_str}_{bs_str}_{order}.json", "w"
        ) as f:
            f.write(json.dumps(query_log, indent=4))

    #################
    # delete the db #
    #################
    del db_test_read
    if not keep_db:
        if os.path.exists(db_test_path):
            shutil.rmtree(db_test_path)


if __name__ == "__main__":
    print(f"Start computation at {time.asctime()}")
    print(f"PID: {PID}")
    print(f"User: {os.getlogin()}")
    print(f"Hostname: {os.uname()[1]}")
    print(f"Putting temp RocksDBs in {config.rocksdb_output_path}")
    print(f"Dataset {config.parquet_path}, size {round(parq_size_b / MiB, 3)} MiB")
    print(f"Number of queries: {n_queries}")
    print()

    print(f"Orderings: {config.orders}")
    print(f"Compressors: {[get_compr_str(c) for c in config.compressors]}")
    print(f"Block sizes: {[get_bs_str(b) for b in config.block_sizes]}")
    print()

    # setup histogram results dictionary
    for m in metrics:
        results[m] = {}
        for b in config.block_sizes:
            bs_str = get_bs_str(b)
            if bs_str not in results[m]:
                results[m][bs_str] = {}
            for c in config.compressors:
                c_str = get_compr_str(c)
                results[m][bs_str][c_str] = 0
    x_compr = list(next(iter(results["compr_ratio"].values())).keys())
    x_blocksizes = list(results["compr_ratio"].keys())

    # get max_size and max_index
    table = pq.read_table(config.parquet_path, columns=["size"])
    table_len = table.num_rows
    index_len = len(str(table_len))
    max_size = 0
    for i in table["size"]:
        size = i.as_py()
        if size > max_size:
            max_size = size

    # create query log directory
    if querylog:
        os.makedirs(f"query_log-{PID}")

    # create queries list
    if n_queries == 0 or n_queries > table_len:
        n_queries = table_len
    queries = list(np.random.permutation(table_len)[:n_queries])

    print(
        "BLOCK_SIZE(KiB),COMPRESSION,ORDER,INSERT_THROUGHPUT(MiB/s),COMPRESSION_RATIO(%),AVG_SST_FILE_SIZE(MiB),SINGLE_GET_THROUGHPUT(MiB/s),MULTI_GET_THROUGHPUT(MiB/S)"
    )
    # run tests
    for block_size in config.block_sizes:
        for compr in config.compressors:
            test_orders = config.orders
            for order in test_orders:
                test_rocksdb(
                    compressor=compr,
                    order=order,
                    block_size=block_size,
                    table_len=table_len,
                    index_len=index_len,
                    max_size=max_size,
                    queries=queries,
                )
    print()

    # create histograms for the results
    if make_charts:
        charts_dir = f"charts_benchmark-{PID}"
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
