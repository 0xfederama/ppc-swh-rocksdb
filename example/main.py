import random
import time

import pyarrow.parquet as pq

import lib

KiB = 1024
MiB = 1024 * 1024 * 1024


if __name__ == "__main__":
    db = lib.DB_PPC("/path/to/test_dir/")
    db.create_db()

    # read the parquet and insert into the db
    parquet_path = "/path/to/file.parquet"
    parquet_file = pq.ParquetFile(parquet_path)
    max_size = 1 * MiB
    keys_to_get = []

    # Test insertion
    ins_time = 0
    tot_size = 0
    for batch in parquet_file.iter_batches(
        columns=[
            "hexsha",
            "max_stars_repo_path",
            "content",
            "size",
            "lang",
        ]
    ):
        batch_list = batch.to_pylist()
        for row in batch_list:
            key = db.make_key(
                row["hexsha"], row["max_stars_repo_path"], int(row["size"]), max_size
            )
            start = time.time()
            db.insert_single(key.encode(), row["content"].encode())
            end = time.time()
            ins_time += end - start
            tot_size += int(row["size"])
            # save 10% of keys for retrieval
            if random.random() < 0.1:
                keys_to_get.append(key.encode())
    print(f"Insertion throughput: {round(tot_size / MiB / ins_time, 2)} MiB/s")

    print("Getting", len(keys_to_get))

    # Test single-get
    sg_time = 0
    got_size = 0
    for key in keys_to_get:
        start = time.time()
        value = db.single_get(key)
        end = time.time()
        sg_time += end - start
        got_size += len(value)
    print(f"Single-get throughput: {round(got_size / MiB / sg_time, 2)} MiB/s")

    # Test multi-get
    start = time.time()
    values = db.multi_get(keys_to_get)
    end = time.time()
    mg_time = end - start
    print(f"Single-get throughput: {round(got_size / MiB / mg_time, 2)} MiB/s")
