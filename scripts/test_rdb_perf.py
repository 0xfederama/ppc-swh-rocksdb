from pyarrow.parquet import ParquetFile
import pandas as pd
import aimrocks
import time
import os

test_type = "put"  # put or get
put_parquet_path = "/disk2/federico/the-stack/small/the-stack-10G.parquet"
get_parquet_path = "/disk2/federico/the-stack/small/the-stack-8M.parquet"
ordered_by = "sha"  # sha or filename
dbs_path = f"/disk2/federico/db/rocksdb_perf_test/{ordered_by}"

KiB = 1024
MiB = 1024 * 1024
GiB = 1024 * 1024 * 1024


def test_get():
    get_db_paths = [
        f"{dbs_path}/db_nocomp_4K",
        f"{dbs_path}/db_zstd_4K",
        f"{dbs_path}/db_zlib_4K",
        f"{dbs_path}/db_snappy_4K",
        f"{dbs_path}/db_nocomp_256K",
        f"{dbs_path}/db_zstd_256K",
        f"{dbs_path}/db_zlib_256K",
        f"{dbs_path}/db_snappy_256K",
        f"{dbs_path}/db_nocomp_4096K",
        f"{dbs_path}/db_zstd_4096K",
        f"{dbs_path}/db_zlib_4096K",
        f"{dbs_path}/db_snappy_4096K",
    ]
    pf = ParquetFile(get_parquet_path)
    dataframes = []
    for batch in pf.iter_batches(columns=["hexsha"]):  # , "max_stars_repo_path"]):
        batch_df = batch.to_pandas()
        dataframes.append(batch_df)
    df = pd.concat(dataframes, ignore_index=True)
    print(
        f"GET test, reading {len(df)} records taken from {get_parquet_path} from DBs in {dbs_path}, ordered by {ordered_by}"
    )
    print(
        "CONTENT_DB,SIZE(GiB),SINGLE_TIME/GET(s),SG_THROUGHPUT(MiB/s),MULTI_TIME/GET(s),MG_THROUGHPUT(MiB/s)"
    )
    nqueries = len(df)
    for content_db_path in get_db_paths:
        print(f"{content_db_path.split('/')[-1]},", end="", flush=True)
        # get db size
        tot_db_size = 0
        for dirpath, _, filenames in os.walk(content_db_path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                if not os.path.islink(fp):
                    fsize = os.path.getsize(fp)
                    tot_db_size += fsize
        print(f"{round(tot_db_size / GiB, 2)},", end="", flush=True)
        opts = aimrocks.Options()
        opts.max_open_files = 40000
        opts.create_if_missing = False
        opts.allow_mmap_reads = True
        opts.paranoid_checks = False
        opts.use_adaptive_mutex = True
        contents_db = aimrocks.DB(content_db_path, opts, read_only=True)
        tot_sg_time = 0
        tot_mg_time = 0
        tot_get_size = 0
        keys_mget = []
        found_sg = 0
        found_mg = 0
        # test single and multi gets
        for i, row in df.iterrows():
            i = int(i)
            sha = str(row["hexsha"])
            if ordered_by == "sha":
                key = sha
            elif ordered_by == "filename":
                filename = str(row["max_stars_repo_path"])
                key = f"{filename}-{sha}"
            keys_mget.append(str.encode(key))
            get_start = time.time()
            got = contents_db.get(str.encode(key))
            get_end = time.time()
            tot_get_size += len(got)
            found_sg += sum(x is not None for x in [got])
            tot_sg_time += get_end - get_start
            if i % 1000 == 0 or (i == len(df) - 1 and len(keys_mget) > 0):
                get_start = time.time()
                gotlist = contents_db.multi_get(keys_mget)
                get_end = time.time()
                tot_mg_time += get_end - get_start
                keys_mget.clear()
                found_mg += sum(x is not None for x in gotlist)
        total_get_size_mb = tot_get_size / MiB
        print(
            f"{round(tot_sg_time/len(df), 5)},{round(total_get_size_mb / tot_sg_time, 3)},",
            end="",
            flush=True,
        )
        if found_sg != nqueries:
            print(f"\nERROR: found {found_sg} out of {nqueries} queries")
        if not (found_sg == found_mg):
            print(f"\nERROR: found numbers differ: {found_sg}, {found_mg}")
        print(
            f"{round(tot_mg_time/len(df), 5)},{round(total_get_size_mb / tot_mg_time, 3)}",
            flush=True,
        )


def test_put():
    pf = ParquetFile(put_parquet_path)
    dataframes = []
    for batch in pf.iter_batches(columns=["hexsha", "content", "max_stars_repo_path"]):
        batch_df = batch.to_pandas()
        dataframes.append(batch_df)
    df = pd.concat(dataframes, ignore_index=True)
    block_sizes = [
        4 * KiB,
        256 * KiB,
        4 * MiB,
    ]
    compressions = [
        (aimrocks.CompressionType.no_compression, "nocomp"),
        (aimrocks.CompressionType.zstd_compression, "zstd"),
        (aimrocks.CompressionType.zlib_compression, "zlib"),
        (aimrocks.CompressionType.snappy_compression, "snappy"),
    ]
    print(
        f"PUT test, reading {len(df)} records taken from {put_parquet_path}, DBs in {dbs_path}, , ordered by {ordered_by}"
    )
    print(
        "BLOCK_SIZE(KiB),COMPRESSION,TOT_PUT_SIZE(MiB),TOT_PUT_TIME(s),AVG_PUT_TIME(s),THROUGHPUT(MiB/s)"
    )
    for block_size in block_sizes:
        for compression in compressions:
            print(f"{block_size / KiB},", end="")
            compr = compression[0]
            compr_str = compression[1]
            print(f"{compr_str},", end="", flush=True)

            # create the test_db
            test_db_path = f"{dbs_path}db_{compr_str}_{int(block_size / KiB)}K"
            opts = aimrocks.Options()
            opts.create_if_missing = True
            opts.error_if_exists = True
            opts.allow_mmap_reads = True
            opts.paranoid_checks = False
            opts.use_adaptive_mutex = True
            opts.table_factory = aimrocks.BlockBasedTableFactory(block_size=block_size)
            opts.compression = compr
            test_db = aimrocks.DB(test_db_path, opts, read_only=False)

            # for each in df, put in test_db
            tot_put_size = 0
            tot_put_time = 0
            batch_write = aimrocks.WriteBatch()
            for i, row in df.iterrows():
                sha = str(row["hexsha"])
                content = str(row["content"])
                if ordered_by == "sha":
                    key = sha
                elif ordered_by == "filename":
                    filename = str(row["max_stars_repo_path"])
                    key = f"{filename}-{sha}"
                tot_put_size += len(content)
                put_start = time.time()
                batch_write.put(str.encode(key), str.encode(content))
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
            tot_put_size_mb = tot_put_size / MiB
            print(f"{round(tot_put_size_mb)},", end="", flush=True)
            print(f"{round(tot_put_time, 3)},", end="", flush=True)
            print(f"{round(tot_put_time/len(df), 5)},", end="", flush=True)
            print(f"{round(tot_put_size_mb/tot_put_time, 3)}", flush=True)

            # delete the test_db
            # import shutil
            # del test_db
            # if os.path.exists(test_db_path):
            #     shutil.rmtree(test_db_path)


if __name__ == "__main__":
    print(f"Starting at {time.asctime()}, pid: {os.getpid()}\n")

    if test_type == "get":
        test_get()
    elif test_type == "put":
        test_put()

    print(f"\nEnding at {time.asctime()}", flush=True)
