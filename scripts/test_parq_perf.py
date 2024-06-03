#!/usr/bin/env python3

import os
import time

from pyarrow.parquet import ParquetFile

parq_uncomp = "/disk2/federico/the-stack/the-stack-dedup_v1_uncomp_none.parquet"
parq_compr = "/disk2/data/the-stack/the-stack-dedup_v1.parquet"

if __name__ == "__main__":
    print(f"Starting at {time.asctime()}, pid: {os.getpid()}")
    start_pf = time.time()
    pf = ParquetFile(parq_uncomp)
    end_pf = time.time()
    print(f"Open uncomp parq file: {round(end_pf - start_pf)}")
    start_read = time.time()
    tot_uncomp_size = 0
    for batch in pf.iter_batches(
        columns=[
            "hexsha",
            "max_stars_repo_path",
            "max_stars_repo_name",
            "content",
            "size",
        ]
    ):
        df = batch.to_pandas()
        del df
    end_read = time.time()
    print(f"Read uncomp parq file {round(end_read - start_read)}")

    print(f"Half time: {time.asctime()}")

    del pf
    start_pf = time.time()
    pf = ParquetFile(parq_compr)
    end_pf = time.time()
    print(f"Open compr parq file: {round(end_pf - start_pf)}")
    start_read = time.time()
    tot_compr_size = 0
    for batch in pf.iter_batches(
        columns=[
            "hexsha",
            "max_stars_repo_path",
            "max_stars_repo_name",
            "content",
            "size",
        ]
    ):
        df = batch.to_pandas()
        del df
    end_read = time.time()
    print(f"Read compr parq file {round(end_read - start_read)}")

    print(f"Ending at {time.asctime()}")
