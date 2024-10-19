import json
import mmap
import os
import subprocess
import time

import pandas as pd
import pyarrow.parquet as pq
import tlsh


parq_size = "10G"  # 5rec, 1M, 8M, 64M, 256M, 1G, 4G, 10G, 200G, dedup_v1, 1G_minsize_4M, 2G_minsize_1M, 10G_minsize_1012K, 24G_minsize_990K
small_parq_path = "/weka1/federico/the-stack/small/the-stack-" + parq_size + ".parquet"
full_parq_path = "/weka1/federico/the-stack/the-stack-" + parq_size + ".parquet"
parq_path = small_parq_path if "dedup_v1" not in parq_size else full_parq_path
parq_size_b = os.path.getsize(parq_path)

txt_contents_path = "/weka1/federico/the-stack/the-stack-dedup_v1-contents.txt"
txt_index_path = "/weka1/federico/the-stack/the-stack-dedup_v1-contents-index.json"
tmp_test_path = "/weka1/federico/db/tmp/"

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


def exec_cmd(cmd, redirect_file=None):
    if redirect_file:
        with open(redirect_file, "w") as outfile:
            start_time = time.time()
            process = subprocess.run(cmd.split(), text=True, stdout=outfile)
            end_time = time.time()
            return end_time - start_time
    else:
        start_time = time.time()
        process = subprocess.run(cmd.split(), capture_output=True, text=True)
        end_time = time.time()
        if process.returncode != 0:  # needed when we capture the output
            print(f"ERROR: {process.stderr}")
        return end_time - start_time


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

    print(f"{compr_str},", end="")

    ##################
    # sort if needed #
    ##################
    sort_start = time.time()
    sorted_df = sort_df(metainfo_df, order, lsh)
    sort_end = time.time()
    sort_time = round(sort_end - sort_start)
    print_lsh = ""
    if order == "fingerprint":
        print_lsh = "-" + lsh
    print(f"{order}{print_lsh},{sort_time},", end="")

    #######################
    # build the test file #
    #######################
    test_contents_path_uncomp = f"{test_filename}.txt"
    test_index_path_uncomp = f"{test_filename}-index.txt"
    metadata_csv_path_uncomp = f"{test_filename}-metadata.csv"
    start_index = 0
    # for each row in df, get from txt_contents and append to test file
    with open(test_index_path_uncomp, "a") as f_index:
        f_index.write("{\n")
        with open(test_contents_path_uncomp, "ab") as f_content:
            for _, row in sorted_df.iterrows():
                sha = str(row["hexsha"])
                coords = txt_index[sha]
                start = coords[0]
                length = coords[1]
                txt_mmap.seek(start)
                content = txt_mmap.read(length)
                f_content.write(content)
                f_index.write(f'"{sha}": [{start_index}, {start_index + length}],\n')
                start_index += length
        f_index.write("}\n")
    # rename and remove columns, print metadata to csv
    sorted_df = sorted_df.drop(columns=["fingerprint"])
    sorted_df = sorted_df.rename(
        columns={
            "filename": "max_stars_repo_path",
            "repo": "max_stars_repo_name",
        }
    )
    sorted_df.to_csv(metadata_csv_path_uncomp, index=False)
    tot_uncomp_size = (
        os.path.getsize(test_contents_path_uncomp)
        + os.path.getsize(test_index_path_uncomp)
        + os.path.getsize(metadata_csv_path_uncomp)
    )
    tot_uncomp_size_mb = tot_uncomp_size / MiB

    if "zstd" in compr_str:
        compr_suffix = ".zst"
    elif "gzip" in compr_str:
        compr_suffix = ".gz"
    elif "snappy" in compr_str:
        compr_suffix = ".snappy"
    else:
        compr_suffix = None

    if compr_str == "no":
        print("100,0,0,0")
    else:
        ################################################
        # compress files and measure compression ratio #
        ################################################
        tot_compr_time = 0
        tot_compr_size = 0
        for file in [
            test_contents_path_uncomp,
            test_index_path_uncomp,
            metadata_csv_path_uncomp,
        ]:
            compr_file = file + compr_suffix
            if "snappy" in compr_str:
                tot_compr_time += exec_cmd(f"{compr_cmd} -c {file}", compr_file)
            else:
                tot_compr_time += exec_cmd(f"{compr_cmd} {file}")
            # measure compressed size and sum
            tot_compr_size += os.path.getsize(compr_file)
        # compute metrics
        compr_size_mb = tot_compr_size / MiB
        compr_size_gb = round(tot_compr_size / GiB, 2)
        compr_ratio = round((compr_size_mb * 100) / tot_uncomp_size_mb, 2)
        compr_speed = round(tot_uncomp_size_mb / tot_compr_time, 2)
        print(f"{compr_ratio},{compr_size_gb},{compr_speed},", end="")

        ################################
        # decompress and measure speed #
        ################################
        decompr_cmd = f"{compr_cmd} -d"
        tot_decomp_time = 0
        for file in [
            test_contents_path_uncomp,
            test_index_path_uncomp,
            metadata_csv_path_uncomp,
        ]:
            compr_file = file + compr_suffix
            if "snappy" in compr_str:
                tot_decomp_time += exec_cmd(f"{decompr_cmd} {compr_file}", file)
            else:
                tot_decomp_time += exec_cmd(f"{decompr_cmd} {compr_file}")
        # compute throughput
        decomp_speed = round(tot_uncomp_size_mb / tot_decomp_time, 2)
        print(f"{round(decomp_speed, 2)}")

    ####################
    # delete the files #
    ####################
    del sorted_df
    for file in [
        test_contents_path_uncomp,
        test_index_path_uncomp,
        metadata_csv_path_uncomp,
    ]:
        if os.path.exists(file):
            os.remove(file)
        if compr_str != "no":
            if os.path.exists(file + compr_suffix):
                os.remove(file + compr_suffix)


if __name__ == "__main__":
    print(f"Start computation at {time.asctime()}")
    print(f"PID: {PID}")
    print(f"User: {os.getlogin()}")
    print(f"Hostname: {os.uname()[1]}")
    print(f"Content txt in {txt_contents_path}")
    print(f"Putting temp files in {tmp_test_path}")
    print(f"Dataset {parq_path}, size {round(parq_size_b / MiB, 3)} MiB")
    print()

    # declare different tests
    orders = [
        # "parquet",  # standard order of the parquet file (by language)
        "filename",
        # "filename_repo",
        # "repo_filename",
        "fingerprint",
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
    for batch in parquet_file.iter_batches():
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

    # print header and execute tests
    print(
        "COMPRESSION,ORDER,SORTING_TIME(s),COMPRESSION_RATIO(%),TOT_COMPR_SIZE(GiB),COMPRESSION_SPEED(MiB/s),DECOMPRESSION_SPEED(MiB/S)"
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
