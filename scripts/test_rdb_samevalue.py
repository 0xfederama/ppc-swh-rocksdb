import json
import os
import shutil
import time

import aimrocks
import matplotlib.pyplot as plt
import numpy as np
import pyarrow.parquet as pq
import tlsh

querylog = False  # True to output queries to file, False to skip it
make_charts = False  # True to create charts, False to skip it
keep_db = False  # True to delete the test dbs, False to skip it
readonly = False  # True to close db and reopen in readonly, False to skip it
drive_type = "HDD"  # HDD to test on HDD, SSD to test on SSD
n_queries = 50000  # number of queries to make on the dbs to test their throughput

parq_size = "10G"  # 5rec, 1M, 8M, 64M, 256M, 1G, 4G, 10G, 200G, dedup_v1, 1G_minsize_4M, 2G_minsize_1M, 10G_minsize_1012K, 24G_minsize_990K
value_size = "8K"  # 4k, 8k, 64k
lipsum_txt = f"../utils/lipsum-{value_size}.txt"

small_parq_path = "/weka1/federico/the-stack/small/the-stack-" + parq_size + ".parquet"
full_parq_path = "/weka1/federico/the-stack/the-stack-" + parq_size + "-zstd.parquet"
parq_path = small_parq_path if "v1" not in parq_size else full_parq_path
if parq_size == "200G":
    parq_path = "/weka1/federico/boffa-200G-py/dataset.parquet"
parq_size_b = os.path.getsize(parq_path)

txt_contents_path = "/weka1/federico/the-stack/the-stack-v1-contents.txt"
txt_index_path = "/weka1/federico/the-stack/the-stack-v1-contents-index.json"
if parq_size == "200G":
    txt_contents_path = "/weka1/federico/boffa-200G-py/contents.txt"
    txt_index_path = "/weka1/federico/boffa-200G-py/contents-index.json"

if drive_type == "HDD":
    tmp_test_path = "/weka1/federico/db/tmp/"
elif drive_type == "SSD":
    tmp_test_path = "/nvme/f.ramacciotti/tmp/"
else:
    print("Drive type must be either HDD or SSD")
    exit()

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
        case "filename_boffa":
            size_len = len(str(max_size))
            size = str(row["size"]).zfill(size_len)
            filename = str(row["filename"])
            if filename is None:
                filename = ""
            key = filename[::-1] + "_" + size + "-" + sha
        case "filename_tosoni":
            size_len = len(str(max_size))
            size = str(row["size"]).zfill(size_len)
            filename = str(row["filename"])
            if filename is None:
                filename = ""
            key = reverse_filename_tosoni(filename) + "_" + size + "-" + sha
        case "tosoni_nopath":
            size_len = len(str(max_size))
            size = str(row["size"]).zfill(size_len)
            filename = str(row["filename"])
            if filename is None:
                filename = ""
            key = reverse_filename_tosoni_nopath(filename) + "_" + size + "-" + sha
        case "lang_filename_tos":
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
    return key


def get_compr_str(compr: tuple[aimrocks.CompressionType, int]):
    c_str = compr[0]
    if compr[0] != aimrocks.CompressionType.no_compression and compr[1] != 0:
        c_str += "-" + str(compr[1])
    return c_str.replace("_compression", "")


def get_bs_str(bs: int):
    return str(round(bs / KiB)) + " KiB"


def reverse_filename_tosoni_nopath(input_path: str):
    if input_path is None:
        return ""
    filename_with_extension = input_path.split("/")[-1]
    transformed_name = filename_with_extension
    if "." in transformed_name:
        filename, extension = filename_with_extension.rsplit(".", 1)
        transformed_name = f"{extension}.{filename}"
    return transformed_name


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


def sort_df(data: list[dict], order: str):
    sorted_data = data[:]
    if order != "parquet":
        match order:
            case "filename_boffa":
                sorted_data.sort(
                    key=lambda x: (
                        x["filename"][::-1] if x["filename"] != None else "",
                        -x["size"],
                    )
                )
            case "filename_tosoni":
                sorted_data.sort(
                    key=lambda x: (
                        (
                            reverse_filename_tosoni(x["filename"])
                            if x["filename"] != None
                            else ""
                        ),
                        -x["size"],
                    )
                )
            case "tosoni_nopath":
                sorted_data.sort(
                    key=lambda x: (
                        (
                            reverse_filename_tosoni_nopath(x["filename"])
                            if x["filename"] != None
                            else ""
                        ),
                        -x["size"],
                    ),
                )
            case "lang_filename_tos":
                sorted_data.sort(
                    key=lambda x: (
                        x["lang"],
                        (
                            reverse_filename_tosoni(x["filename"])
                            if x["filename"] != None
                            else ""
                        ),
                    )
                )
            case "filename_repo":
                sorted_data.sort(key=lambda x: (x["filename"][::-1], x["repo"]))
            case "repo_filename":
                sorted_data.sort(key=lambda x: (x["repo"], x["filename"][::-1]))
    return sorted_data


def test_rocksdb(
    metadata_list: list[dict],
    compressor: tuple[aimrocks.CompressionType, int],
    order: str,
    block_size: int,
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
    print(f"{block_size/KiB},{compr_str},", end="")

    ##################
    # sort if needed #
    ##################
    sort_start = time.time()
    sorted_df = sort_df(metadata_list, order)
    sort_end = time.time()
    sort_time = round(sort_end - sort_start)
    print(f"{order},{sort_time},", end="")

    #####################
    # build the test db #
    #####################
    tot_insert_time = 0
    index_len = len(str(len(metadata_list)))
    batch_size = 10000
    ins_size = 0
    # for each row in df, read lipsum and insert in test_db
    # read lorem ipsum value
    with open(lipsum_txt, "r") as f:
        lipsum = f.read()
    batch_write = aimrocks.WriteBatch()
    for i, row in enumerate(sorted_df):
        content = lipsum
        ins_size += len(content)
        key = make_key(order, index_len, max_size, i, row)
        batch_write.put(str.encode(key), content)
        if int(i) % batch_size == 0 or (
            i == len(sorted_df) - 1 and batch_write.count() > 0  # last iteration
        ):
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
    query_log = []
    found_sg = 0
    found_mg = 0
    got_size = 0
    ind_query = 1
    keys_mget = []
    tot_sg_time = 0
    tot_mg_time = 0
    index_len = len(str(len(metadata_list)))
    for i, row in enumerate(metadata_list):
        if int(i) in queries:
            key = make_key(order, index_len, max_size, i, row)
            query_log.append(str(key))
            keys_mget.append(str.encode(key))
            # test single get
            start_sg_time = time.time()
            got = db_test_read.get(str.encode(key))
            end_sg_time = time.time()
            tot_sg_time += end_sg_time - start_sg_time
            got_size += len(got)
            found_sg += sum(x is not None for x in [got])
            ind_query += 1
        # test multi get
        if ind_query % 100 == 0 or (i == len(metadata_list) - 1 and len(keys_mget) > 0):
            start_mg_time = time.time()
            gotlist = db_test_read.multi_get(keys_mget)
            end_mg_time = time.time()
            keys_mget.clear()
            ind_query = 1
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
            f"query_log-{parq_size}-{PID}/{compr_str}_{bs_str}_{order}.json", "w"
        ) as f:
            f.write(json.dumps(query_log, indent=4))

    #################
    # delete the db #
    #################
    del db_test_read
    del sorted_df
    if not keep_db:
        if os.path.exists(db_test_path):
            shutil.rmtree(db_test_path)


if __name__ == "__main__":
    print(f"Start computation at {time.asctime()}")
    print(f"PID: {PID}")
    print(f"User: {os.getlogin()}")
    print(f"Hostname: {os.uname()[1]}")
    print(f"Reading values from {lipsum_txt}")
    print(f"Putting temp RocksDBs in {tmp_test_path}")
    print(f"Dataset {parq_path}, size {round(parq_size_b / MiB, 3)} MiB")
    print()

    # declare different tests
    orders = [
        # "parquet",  # standard order of the parquet file (by language)
        # "filename_boffa",
        # "filename_tosoni",
        # "tosoni_nopath",
        # "lang_filename_tos",
        # "filename_repo",
        # "repo_filename",
    ]
    compressors = [
        # (aimrocks.CompressionType.no_compression, 0),
        # (aimrocks.CompressionType.zstd_compression, 3),
        # (aimrocks.CompressionType.zstd_compression, 12),
        # (aimrocks.CompressionType.zstd_compression, 22),
        (aimrocks.CompressionType.zlib_compression, 6),
        # (aimrocks.CompressionType.zlib_compression, 9),
        # (aimrocks.CompressionType.snappy_compression, 0),
    ]
    block_sizes = [
        4 * KiB,
        # 8 * KiB,
        # 16 * KiB,
        # 32 * KiB,
        # 64 * KiB,
        # 128 * KiB,
        # 256 * KiB,
        # 512 * KiB,
        # 1 * MiB,
        # 4 * MiB,
        # 10 * MiB,
    ]
    print(f"Orderings: {orders}")
    print(f"Compressors: {[get_compr_str(c) for c in compressors]}")
    print(f"Block sizes: {[get_bs_str(b) for b in block_sizes]}")
    print()

    # read parquet to create metadata dataframe
    start_reading = time.time()
    metadata_list = []
    max_size = 0
    parquet_file = pq.ParquetFile(parq_path)
    for batch in parquet_file.iter_batches(
        columns=[
            "hexsha",
            "max_stars_repo_path",
            # "max_stars_repo_name",
            "size",
            "lang",
        ]
    ):
        try:
            batch = batch.rename_columns(
                {
                    "hexsha": "hexsha",
                    "filename": "max_stars_repo_path",
                    # "repo": "max_stars_repo_name",
                    "size": "size",
                    "lang": "lang",
                }
            )
            batch_list = batch.to_pylist()
            metadata_list += batch_list
        except Exception as e:
            print(e)
    # concatenate the results
    end_reading = time.time()
    print(
        f"Reading parquet and computing fingerprints: {round(end_reading - start_reading)} s\n"
    )

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

    # create query log directory
    if querylog:
        os.makedirs(f"query_log-{parq_size}-{PID}")

    # create queries list
    if n_queries == 0 or n_queries > len(metadata_list):
        n_queries = len(metadata_list)
    queries = list(np.random.permutation(len(metadata_list))[:n_queries])

    print(
        "BLOCK_SIZE(KiB),COMPRESSION,ORDER,SORTING_TIME(s),INSERT_THROUGHPUT(MiB/s),COMPRESSION_RATIO(%),AVG_SST_FILE_SIZE(MiB),SINGLE_GET_THROUGHPUT(MiB/s),MULTI_GET_THROUGHPUT(MiB/S)"
    )
    # run tests
    for block_size in block_sizes:
        for compr in compressors:
            test_orders = orders
            # if compr[0] == aimrocks.CompressionType.no_compression:
            #     # without compression the order and the lsh are useless
            #     test_orders = ["parquet"]
            for order in test_orders:
                test_rocksdb(
                    metadata_list=metadata_list,
                    compressor=compr,
                    order=order,
                    block_size=block_size,
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
