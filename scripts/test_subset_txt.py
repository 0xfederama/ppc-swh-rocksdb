#!/usr/bin/env python3

import json
import mmap
import os
import random
import time

from pyarrow.parquet import ParquetFile

KiB = 1024
MiB = 1024 * 1024
GiB = 1024 * 1024 * 1024

txt_size = float("inf")
txt_size_str = "dedup_v1"
get_calls = 1000000  # number of "get" calls to be done on the txt file

parquet_path = "/disk2/data/the-stack/the-stack-dedup_v1.parquet"

if __name__ == "__main__":
    # build and write to file
    file_path = f"/disk2/federico/the-stack/other_formats/the-stack-{txt_size_str}.txt"
    if not os.path.exists(file_path):
        print(f"Building the file {txt_size_str}")
        start_put = time.time()
        sha_sizes = {}  # made of { sha: (start_index, size) }
        with open(file_path, "a") as f:
            tot_size = 0
            pf = ParquetFile(parquet_path)
            for batch in pf.iter_batches(columns=["hexsha", "size", "content"]):
                for i in range(len(batch["hexsha"])):
                    sha = str(batch["hexsha"][i])
                    content = str(batch["content"][i])
                    size = int(str(batch["size"][i]))
                    sha_sizes[sha] = (tot_size, size)
                    tot_size += size
                    f.write(content)
                    if tot_size >= txt_size:
                        break
                if tot_size >= txt_size:
                    break
        file_index = (
            f"/disk2/federico/the-stack/other_formats/txt-index_{txt_size_str}.json"
        )
        with open(file_index, "w") as f:
            f.write(json.dumps(sha_sizes, indent=4))
        end_put = time.time()
        tot_put_time = end_put - start_put
        print(f"  It took {round(tot_put_time)} s")
    else:
        print(f"File {txt_size_str} already exists")
        file_index = (
            f"/disk2/federico/the-stack/other_formats/txt-index_{txt_size_str}.json"
        )
        with open(file_index, "r") as f:
            sha_sizes = json.load(f)

    # test get with mmap
    tot_get_time = 0
    tot_get_size = 0
    shas = list(sha_sizes.keys())
    if get_calls < len(shas):
        shas = shas[:get_calls]
    random.shuffle(shas)
    print(f"Testing the file retrieving {len(shas)} contents")
    with open(file_path, "r") as f:
        with mmap.mmap(f.fileno(), length=0, access=mmap.PROT_READ) as f_mmap:
            for sha in shas:
                start_get = time.time()
                coords = sha_sizes[sha]
                start = coords[0]
                length = coords[1]
                f_mmap.seek(start)
                content = f_mmap.read(length)
                end_get = time.time()
                tot_get_time += end_get - start_get
                tot_get_size += length

    print("GET test:")
    print(f"  Total time: {round(tot_get_time, 3)} s")
    print(f"  Time per get: {tot_get_time / len(shas)} s")
    print(f"  Throughput: {round((tot_get_size / MiB) / tot_get_time, 3)} MiB/s")
