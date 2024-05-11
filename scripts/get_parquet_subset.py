#!/usr/bin/env python3

import os
import time
from pyarrow.parquet import ParquetFile
import pandas as pd

KiB = 1024
MiB = 1024 * 1024
GiB = 1024 * 1024 * 1024
size = 200 * GiB
size_str = "200G"
parquet_path = "/disk2/data/the-stack/the-stack-dedup_v1.parquet"

if __name__ == "__main__":
    print(f"PID: {os.getpid()}")
    start_time = time.time()

    # assuming 600 rows = 1 MB (from tests)
    size_mb = size / MiB
    batch_size = 600
    tot_rows_to_read = size_mb * 600
    reads = tot_rows_to_read / batch_size
    # if size_str ends with a G (reading gigabytes), read more rows at a time, otherwise proceed with 1MB of rows
    if size_str[-1] == "G":
        reads = reads / 1024
        batch_size = batch_size * 1024
    print(f"{reads} read of {batch_size} batch size rows")

    # read the parquet
    pf = ParquetFile(parquet_path)
    parquet_iterator = pf.iter_batches(batch_size=batch_size)
    data = []
    for i in range(int(reads)):
        # don't iterate on the iterator itself because it will iterate until the file is completely read
        n_rows = next(parquet_iterator)
        to_pd = n_rows.to_pandas()
        data.append(to_pd)

    df = pd.concat(data, ignore_index=True)

    df.to_parquet(f"/disk2/federico/the-stack/the-stack-small_{size_str}.parquet")

    end_time = time.time()
    tot_time = end_time - start_time

    print(f"Parquet created in {round(tot_time)} s")
