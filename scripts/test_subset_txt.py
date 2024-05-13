#!/usr/bin/env python3

import mmap
import random
import time
from pyarrow.parquet import ParquetFile
import pandas as pd
import os

KiB = 1024
MiB = 1024 * 1024
GiB = 1024 * 1024 * 1024
txt_size = 10 * GiB
txt_size_str = "10G"
parquet_path = "/disk2/data/the-stack/the-stack-dedup_v1.parquet"
# parquet_path = "/disk2/federico/the-stack/the-stack-small_5rec.parquet"

if __name__ == "__main__":
    pf = ParquetFile(parquet_path)
    # assuming 600 rows = 1 MB (from tests), dividev by 2.5 due to parquet's compression ratio
    batch_size = round(600 / 2.5) + 1
    tot_rows_to_read = (txt_size / MiB) * (600 / 2.5)
    reads = tot_rows_to_read / batch_size
    parquet_iterator = pf.iter_batches(
        columns=["hexsha", "size", "content"],
        batch_size=batch_size,
    )
    data = []
    for i in range(int(reads)):
        # don't iterate on the iterator itself because it will iterate until the file is completely read
        n_rows = next(parquet_iterator)
        to_pd = n_rows.to_pandas()
        data.append(to_pd)

    df = pd.concat(data, ignore_index=True)

    # build and write to file
    print("Building the file")
    start_put = time.time()
    file_path = f"/disk2/federico/the-stack/the-stack-small_{txt_size_str}.txt"
    if os.path.exists(file_path):
        os.remove(file_path)
    sha_sizes = []  # made of tuples (sha, start_index, size)
    with open(file_path, "a") as f:
        index = 0
        for i, row in df.iterrows():
            content = str(row["content"])
            size = len(content)
            sha = str(row["hexsha"])
            sha_sizes.append((sha, index, size))
            index += size
            f.write(content)
    end_put = time.time()
    tot_put_time = end_put - start_put
    print(f"  It took {round(tot_put_time)} s")

    # test get with mmap
    print("Testing the file")
    tot_get_time = 0
    tot_get_size = index
    random.shuffle(sha_sizes)
    with open(file_path, "r") as f:
        with mmap.mmap(f.fileno(), length=0, access=mmap.PROT_READ) as f_mmap:
            for sha, start, length in sha_sizes:
                start_get = time.time()
                f_mmap.seek(start)
                got = f_mmap.read(length)
                end_get = time.time()
                tot_get_time += end_get - start_get

    print("GET test:")
    print(f"  Total time: {round(tot_get_time, 3)} s")
    print(f"  Time per get: {tot_get_time / len(sha_sizes)} s")
    print(f"  Throughput: {round((tot_get_size / MiB) / tot_get_time, 3)} MiB/s")
