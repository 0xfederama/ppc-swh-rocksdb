#!/usr/bin/env python3

import os
import time
import pandas as pd
from pyarrow.parquet import ParquetFile

KiB = 1024
MiB = 1024 * 1024
GiB = 1024 * 1024 * 1024
size = 1 * GiB
size_str = "1G"
bigfiles_min_size = 4 * MiB
run = "bigfiles"  # bigfiles or standard

parquet_path = "/disk2/data/the-stack/the-stack-dedup_v1.parquet"


def get_parq_bigfiles():
    tot_size = 0
    tot_files = 0
    pf = ParquetFile(parquet_path)
    dataframes = []
    for batch in pf.iter_batches():
        batch_df = batch.to_pandas()
        for i, row in batch_df.iterrows():
            cont_size = int(row["size"])
            if cont_size > bigfiles_min_size:
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
    df.to_parquet(
        f"/disk2/federico/the-stack/the-stack-small_{size_str}_bigfiles.parquet"
    )


def get_parq_standard():
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


if __name__ == "__main__":
    print(f"Starting at {time.asctime()}, pid: {os.getpid()}")
    print(f"Getting a subset of {size_str} from {parquet_path}")
    start_time = time.time()

    match run:
        case "standard":
            get_parq_standard()
        case "bigfiles":
            get_parq_bigfiles()

    end_time = time.time()
    tot_time = end_time - start_time

    print(f"Parquet created in {round(tot_time)} s")
    print(f"Ending at {time.asctime()}")
