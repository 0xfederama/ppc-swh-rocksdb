#!/usr/bin/env python3

import os
import time
import pandas as pd
from pyarrow.parquet import ParquetFile

KiB = 1024
MiB = 1024 * 1024
GiB = 1024 * 1024 * 1024

size = float("inf")  # float('inf') to take all the files
size_str = "inf"
minsize = 1 * MiB  # 0 to take files with all sizes
minsize_str = "1M"

parq_path = "/disk2/data/the-stack/the-stack-dedup_v1.parquet"

if __name__ == "__main__":
    print(f"Starting at {time.asctime()}, pid: {os.getpid()}")
    print(
        f"Getting a subset of {size_str if size != float('inf') else 'inf'} from {parq_path} with min size {minsize_str}"
    )
    start_time = time.time()

    tot_size = 0
    tot_files = 0
    pf = ParquetFile(parq_path)
    dataframes = []
    for batch in pf.iter_batches():
        batch_df = batch.to_pandas()
        for i, row in batch_df.iterrows():
            cont_size = int(row["size"])
            if cont_size > minsize:
                dataframes.append(row.to_dict())
                tot_size += cont_size
                tot_files += 1
                if tot_size >= size:
                    break
        if tot_size >= size:
            break

    df = pd.DataFrame(dataframes)
    print(f"Total size reached: {round(tot_size / GiB, 3)} GiB")
    print(f"Total number of files: {tot_files}")
    if size == float("inf"):
        size_str = str(round(tot_size / GiB)) + "G"
    if minsize == 0:
        filename = f"/disk2/federico/the-stack/the-stack-{size_str}.parquet"
    else:
        filename = f"/disk2/federico/the-stack/the-stack-{size_str}_minsize_{minsize_str}.parquet"
    df.to_parquet(filename, compression=None)

    end_time = time.time()
    tot_time = end_time - start_time

    print(f"Parquet {filename} created in {round(tot_time)} s")
    print(f"Ending at {time.asctime()}")
