import json
import os
import time

from pyarrow.parquet import ParquetFile

KiB = 1024
MiB = 1024 * 1024
GiB = 1024 * 1024 * 1024

txt_size = float("inf")

parquet_path = "/home/f.ramacciotti/ppc-swh-rocksdb/utils/the-stack-8M.parquet"
contents_path = f"/home/f.ramacciotti/ppc-swh-rocksdb/utils/the-stack-8M-contents.txt"
contents_index_path = (
    f"/home/f.ramacciotti/ppc-swh-rocksdb/utils/the-stack-8M-contents-index.json"
)

if __name__ == "__main__":
    print(f"Starting at {time.asctime()}")

    print(
        f"Reading from {parquet_path}, writing to {contents_path} and {contents_index_path}"
    )

    if os.path.exists(contents_path):
        print(f"File {contents_path} already exists")
        exit()

    print(f"Building the file {contents_path}")
    sha_sizes = {}  # made of { sha: (start_index, size) }
    tot_put_time = 0

    with open(contents_path, "a+") as f:
        tot_size = 0
        pf = ParquetFile(parquet_path)
        for batch in pf.iter_batches(columns=["hexsha", "size", "content"]):
            for i in range(len(batch["hexsha"])):
                sha = str(batch["hexsha"][i])
                content = str(batch["content"][i])
                size = int(str(batch["size"][i]))
                sha_sizes[sha] = (tot_size, size)
                tot_size += size
                start_put = time.time()
                f.write(content)
                end_put = time.time()
                tot_put_time += end_put - start_put

    print(f"Building the file {contents_index_path}")
    with open(contents_index_path, "w") as f:
        f.write(json.dumps(sha_sizes, indent=4))

    print(
        f"Total time to write {round(tot_put_time, 3)} s, {round((tot_size / MiB) / tot_put_time, 3)} MiB/s"
    )

    print(f"Ending at {time.asctime()}")
