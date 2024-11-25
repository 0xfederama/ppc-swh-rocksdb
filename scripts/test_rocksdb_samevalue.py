import os
import random
import shutil
import time

import aimrocks
import numpy as np

KiB = 1024
MiB = 1024 * 1024
GiB = 1024 * 1024 * 1024
PID = os.getpid()

keep_db = False  # True to delete the test dbs, False to skip it
n_queries = 10000  # number of queries to make on the dbs to test their throughput
db_size = 10 * GiB
value_source = "random"  # "random" for random values, "lipsum" to read lorem ipsum
tmp_test_path = "/weka1/federico/db/tmp/"


def get_compr_str(compr: tuple[aimrocks.CompressionType, int]):
    c_str = compr[0]
    if compr[0] != aimrocks.CompressionType.no_compression and compr[1] != 0:
        c_str += "-" + str(compr[1])
    return c_str.replace("_compression", "")


def get_size_str(sz: int):
    return str(round(sz / KiB)) + " KiB"


def test_rocksdb(
    value_size: int,
    compressor: tuple[aimrocks.CompressionType, int],
    block_size: int,
):
    ######################
    # create the test db #
    ######################
    compr = compressor[0]
    level = compressor[1]
    db_test_path = (
        f"{tmp_test_path}db_{str(compr)}_{str(block_size)}_{int(time.time())}"
    )
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
        # opts.compression_opts["level"] = level
    opts.table_factory = aimrocks.BlockBasedTableFactory(block_size=block_size)
    db_test = aimrocks.DB(db_test_path, opts, read_only=False)

    compr_str = get_compr_str(compressor)
    print(f"{value_size/KiB},{block_size/KiB},{compr_str},", end="")

    #####################
    # build the test db #
    #####################
    if value_source == "random":
        alphabeth = "abcdefghijklmnopqrstuvwxyz"
        value = ""
        for i in range(value_size):
            value += random.choice(alphabeth)
        value = str.encode(value)
    elif value_source == "lipsum":
        value_str = value_size / KiB
        lipsum_filename = f"utils/lipsum-{value_str}k.txt"
        with open(lipsum_filename, "rb") as f:
            value = f.read()

    tot_insert_time = 0
    # tot_uncomp_size = 0
    num_values = int(db_size / value_size)
    index_len = len(str(num_values))
    ins_size = 0
    batch_size = 10000
    batch_write = aimrocks.WriteBatch()
    for i in range(num_values):
        key = str(i).zfill(index_len)
        batch_write.put(str.encode(key), value)
        ins_size += len(value)
        # tot_uncomp_size += (value_size + len(key) * 8)
        if int(i) % batch_size == 0 or (
            i == num_values - 1 and batch_write.count() > 0  # last iteration
        ):
            start_write = time.time()
            db_test.write(batch_write)
            end_write = time.time()
            tot_insert_time += end_write - start_write
            batch_write.clear()
    # compute throughput
    ins_thr = round(ins_size / MiB / tot_insert_time, 2)
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
    compr_ratio = round((tot_db_size * 100) / db_size, 2)
    compr_ratio_ssts = round((tot_sst_size * 100) / db_size, 2)
    avg_sst_size_mb = (
        round((tot_sst_size / MiB) / tot_sst_files, 2) if tot_sst_files != 0 else 0
    )
    print(f"{compr_ratio} ({compr_ratio_ssts} no logs),{avg_sst_size_mb},", end="")

    ################
    # query the db #
    ################
    query_log = []
    found_sg = 0
    found_mg = 0
    got_size = 0
    ind_query = 1
    keys_mget = []
    tot_sg_time = 0
    tot_mg_time = 0
    for i in range(num_values):
        if int(i) in queries:
            key = str(i).zfill(index_len)
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
        if ind_query % 100 == 0 or (i == num_values - 1 and len(keys_mget) > 0):
            start_mg_time = time.time()
            gotlist = db_test.multi_get(keys_mget)
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
    print(f"{round(sg_thr, 2)},{round(mg_thr, 2)}")

    #################
    # delete the db #
    #################
    del db_test
    if not keep_db:
        if os.path.exists(db_test_path):
            shutil.rmtree(db_test_path)


if __name__ == "__main__":
    print(f"Start computation at {time.asctime()}")
    print(f"PID: {PID}")
    print(f"User: {os.getlogin()}")
    print(f"Hostname: {os.uname()[1]}")
    print(f"Putting temp RocksDBs in {tmp_test_path}")
    print(f"Value source: {value_source}")
    print(f"Final db size: {round(db_size / GiB, 2)} GiB")
    print()

    # declare different tests
    value_sizes = [
        # 1 * KiB,
        4 * KiB,
        8 * KiB,
        # 16 * KiB,
        64 * KiB,
    ]
    compressors = [
        # (aimrocks.CompressionType.no_compression, 0),
        (aimrocks.CompressionType.zstd_compression, 3),
        # (aimrocks.CompressionType.zstd_compression, 12),
        (aimrocks.CompressionType.zstd_compression, 22),
        # (aimrocks.CompressionType.zlib_compression, 6),
        # (aimrocks.CompressionType.zlib_compression, 9),
        # (aimrocks.CompressionType.snappy_compression, 0),
    ]
    block_sizes = [
        4 * KiB,
        # 8 * KiB,
        16 * KiB,
        # 32 * KiB,
        64 * KiB,
        # 128 * KiB,
        # 256 * KiB,
        512 * KiB,
        # 1 * MiB,
    ]
    print(f"Value sizes: {[get_size_str(b) for b in value_sizes]}")
    print(f"Compressors: {[get_compr_str(c) for c in compressors]}")
    print(f"Block sizes: {[get_size_str(b) for b in block_sizes]}")
    print()

    # take biggest value size (for less key-pairs) and create queries list
    num_values = int(db_size / value_sizes[-1])
    if n_queries == 0 or n_queries > num_values / 2:
        n_queries = num_values / 2
        print(f"Capping queries to {n_queries}")
    queries = list(np.random.permutation(num_values)[:n_queries])

    # run tests
    print(
        "VALUE_SIZE(KiB),BLOCK_SIZE(KiB),COMPRESSION,INSERT_THROUGHPUT(MiB/s),COMPRESSION_RATIO(%),AVG_SST_FILE_SIZE(MiB),SINGLE_GET_THROUGHPUT(MiB/s),MULTI_GET_THROUGHPUT(MiB/S)"
    )
    for value_size in value_sizes:
        print()
        for block_size in block_sizes:
            for compr in compressors:
                test_rocksdb(
                    value_size=value_size,
                    compressor=compr,
                    block_size=block_size,
                )
    print()

    print(f"End computation at {time.asctime()}")
