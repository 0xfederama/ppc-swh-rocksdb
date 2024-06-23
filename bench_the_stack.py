import json
import mmap
import os
import shutil
import subprocess
import time

import aimrocks
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import tlsh

querylog = False
benchmark_mode = (
    "access"  # "backup" (output to single file) or "access" (output to rocksdb)
)

"""
The parquet file is the file for which the db_test is created (storing index and content).
The db_contents is already created, and it is static, with only sha and content.
"""
parq_size = "10G"  # 5rec, 1M, 8M, 64M, 256M, 1G, 4G, 10G, 200G, dedup_v1, 1G_minsize_4M, 2G_minsize_1M, 10G_minsize_1012K, 24G_minsize_990K
small_parq_path = "/disk2/federico/the-stack/small/the-stack-" + parq_size + ".parquet"
full_parq_path = "/disk2/federico/the-stack/the-stack-" + parq_size + ".parquet"
parq_path = small_parq_path if "dedup_v1" not in parq_size else full_parq_path
parq_size_b = round(os.stat(parq_path).st_size)

txt_contents_path = "/disk2/federico/the-stack/the-stack-dedup_v1-contents.txt"
txt_index_path = "/disk2/federico/the-stack/the-stack-dedup_v1-contents-index.json"
tmp_test_path = "/disk2/federico/db/tmp/"

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


def exec_cmd(cmd, redirect_file: None):
    if redirect_file:
        with open(redirect_file, "w") as outfile:
            process = subprocess.run(cmd.split(), text=True, stdout=outfile)
    else:
        process = subprocess.run(cmd.split(), capture_output=True, text=True)
        if process.returncode != 0:  # needed when we capture the output
            print(f"ERROR: {process.stderr}")


def test_backup(
    txt_mmap: mmap.mmap,
    txt_index: dict[str, dict],
    metainfo_df: pd.DataFrame,
    compressor: tuple[str, str],
    order: str,
    lsh: str,
):
    ##########################
    # create the backup file #
    ##########################
    compr_cmd = compressor[0]
    compr_str = compressor[1]
    test_filename = f"{tmp_test_path}txt_{parq_size}_{compr_str}_{int(time.time())}"
    test_contents_path_uncomp = f"{test_filename}.txt"

    print(f"{compr_str},", end="")

    ##################
    # sort if needed #
    ##################
    sorted_df = sort_df(metainfo_df, order, lsh)
    print_lsh = ""
    if order == "fingerprint":
        print_lsh = "-" + lsh
    print(f"{order}{print_lsh},", end="")

    #######################
    # build the test file #
    #######################
    tot_ins_time = 0
    # for each row in df, get from txt_contents and append to test file
    with open(test_contents_path_uncomp, "ab") as f:
        for _, row in sorted_df.iterrows():
            sha = str(row["hexsha"])
            coords = txt_index[sha]
            start = coords[0]
            length = coords[1]
            txt_mmap.seek(start)
            content = txt_mmap.read(length)
            start_ins = time.time()
            try:
                f.write(content)
            except Exception as e:
                print(e)
                print(
                    f"Exception, content ends with: {content[-10:]} (to string {str(content[-10:])}), start {start}, length {length}"
                )
            end_ins = time.time()
            tot_ins_time += end_ins - start_ins
    # compute throughput
    uncomp_size_mb = os.stat(test_contents_path_uncomp).st_size / MiB
    ins_thr = round(uncomp_size_mb / tot_ins_time, 2)
    print(f"{ins_thr},", end="")

    ###############################################
    # compress file and measure compression ratio #
    ###############################################
    test_contents_path_compr = test_contents_path_uncomp
    if compr_str == "no":
        print("100,0,0,", end="")
    else:
        compr_cmd_opt = compr_cmd
        if "zstd" in compr_str:
            compr_suffix = ".zst"
        elif "gzip" in compr_str:
            compr_suffix = ".gz"
        elif "snappy" in compr_str:
            compr_cmd_opt += " -c"
            compr_suffix = ".snappy"
        test_contents_path_compr = test_contents_path_uncomp + compr_suffix
        redirect = None
        if "snappy" in compr_str:
            redirect = test_contents_path_compr
        start_compr = time.time()
        exec_cmd(f"{compr_cmd_opt} {test_contents_path_uncomp}", redirect_file=redirect)
        end_compr = time.time()
        tot_compr_time = end_compr - start_compr
        # compute metrics
        compr_size_mb = os.stat(test_contents_path_compr).st_size / MiB
        compr_size_gb = round(os.stat(test_contents_path_compr).st_size / GiB, 2)
        compr_ratio = round((compr_size_mb * 100) / uncomp_size_mb, 2)
        compr_speed = round(uncomp_size_mb / tot_compr_time, 2)
        print(f"{compr_ratio},{compr_size_gb},{compr_speed},", end="")

    ###############################
    # measure decompression speed #
    ###############################
    if compr_str == "no":
        print("0")
    else:
        decompr_cmd = f"{compr_cmd} -d"
        redirect = None
        if "snappy" in compr_str:
            redirect = test_contents_path_uncomp
        start_decompr = time.time()
        exec_cmd(f"{decompr_cmd} {test_contents_path_compr}", redirect_file=redirect)
        end_decompr = time.time()
        tot_decompr_time = end_decompr - start_decompr
        # compute throughput
        decompr_thr = round(uncomp_size_mb / tot_decompr_time, 2)
        print(f"{round(decompr_thr, 2)}")

    ####################
    # delete the files #
    ####################
    del sorted_df
    if os.path.exists(test_contents_path_uncomp):
        os.remove(test_contents_path_uncomp)
    if os.path.exists(test_contents_path_compr):
        os.remove(test_contents_path_compr)


def test_rocksdb(
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
    db_test_path = f"{tmp_test_path}db_{parq_size}_{str(compr)}_{str(block_size)}_{int(time.time())}"
    opts = aimrocks.Options()
    opts.create_if_missing = True
    opts.error_if_exists = True
    # options to make db faster
    opts.allow_mmap_reads = True
    opts.paranoid_checks = False
    opts.use_adaptive_mutex = True
    # compression and block
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
    sorted_df = sort_df(metainfo_df, order, lsh)
    print_lsh = ""
    if order == "fingerprint":
        print_lsh = "-" + lsh
    print(f"{order}{print_lsh},", end="")

    #####################
    # build the test db #
    #####################
    tot_insert_time = 0
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
        key = make_key(order, index_len, max_size, i, row, lsh)
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
            if not os.path.islink(fp):
                fsize = os.path.getsize(fp)
                tot_db_size += fsize
                if f.endswith(".sst"):
                    tot_sst_size += fsize
                    tot_sst_files += 1
    compr_ratio = round((tot_db_size * 100) / parq_size_b, 2)
    total_db_size_gb = round(tot_db_size / GiB, 2)
    avg_sst_size_mb = (
        round((tot_sst_size / MiB) / tot_sst_files, 2) if tot_sst_files != 0 else 0
    )
    results["compr_ratio"][bs_str][compr_str] = compr_ratio
    print(f"{compr_ratio},{total_db_size_gb},{avg_sst_size_mb},", end="")

    ########################
    # measure access times #
    ########################
    n_queries = 2500  # 0: query entire db, X: make X queries
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
            key = make_key(order, index_len, max_size, i, row, lsh)
            query_log.append(str(key))
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
        if ind_query % 1000 == 0 or (i == len(metainfo_df) - 1 and len(keys_mget) > 0):
            start_mg_time = time.time()
            gotlist = db_test.multi_get(keys_mget)
            end_mg_time = time.time()
            keys_mget.clear()
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
            f"query_log-{parq_size}-{PID}/{compr_str}_{bs_str}_{order}_{lsh}.json", "w"
        ) as f:
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
    print(f"Putting temp RocksDBs in {tmp_test_path}")
    print(f"Dataset {parq_path}, size {round(parq_size_b / MiB, 3)} MiB")
    print(f"Benchmark mode: {benchmark_mode}")
    print()

    # declare different tests
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
    txt_start = time.time()
    txt_contents_file = open(txt_contents_path, "r")
    txt_mmap = mmap.mmap(txt_contents_file.fileno(), length=0, access=mmap.PROT_READ)
    with open(txt_index_path, "r") as f:
        txt_index = json.load(f)
    txt_end = time.time()
    print(f"Opened contents txt and read txt index: {round(txt_end - txt_start)} s")

    # read parquet to create metadata dataframe
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

    if benchmark_mode == "access":
        print(
            "BLOCK_SIZE(KiB),COMPRESSION,ORDER,INSERT_THROUGHPUT(MiB/s),COMPRESSION_RATIO(%),TOT_SIZE(GiB),AVG_SST_FILE_SIZE(MiB),SINGLE_GET_THROUGHPUT(MiB/s),MULTI_GET_THROUGHPUT(MiB/S)"
        )
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
            # 32 * KiB,
            64 * KiB,
            # 128 * KiB,
            256 * KiB,
            # 512 * KiB,
            # 1 * MiB,
            # 4 * MiB,
            # 10 * MiB,
        ]

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
    elif benchmark_mode == "backup":
        print(
            "COMPRESSION,ORDER,INSERT_THROUGHPUT(MiB/s),COMPRESSION_RATIO(%),TOT_COMPR_SIZE(GiB),COMPRESSION_SPEED(MiB/s),DECOMPRESSION_SPEED(MiB/S)"
        )
        compressors = [
            ("no", "no"),
            ("zstd -3 -f", "zstd-3"),
            ("zstd -12 -f", "zstd-12"),
            ("zstd --ultra -22 -M1024MB --long=30 --adapt -f", "zstd-22"),
            ("gzip -6 -k -f", "gzip-6"),
            ("gzip -9 -k -f", "gzip-9"),
            ("python3 -m snappy", "snappy"),
        ]
        for compr in compressors:
            test_orders = orders
            test_fingerprints = ["no_lsh"]
            # if compr[0] == "no":
            #     # without compression the order and the lsh are useless
            #     test_orders = ["parquet"]
            for order in test_orders:
                if order == "fingerprint":
                    test_fingerprints = fingerprints
                for lsh in test_fingerprints:
                    test_backup(
                        txt_mmap=txt_mmap,
                        txt_index=txt_index,
                        metainfo_df=metainfo_df,
                        compressor=compr,
                        order=order,
                        lsh=lsh,
                    )

    print(f"\nEnd computation at {time.asctime()}")
