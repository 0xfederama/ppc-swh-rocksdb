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
# txt_size_str = "10G"
get_calls = 10000  # number of "get" calls to be done on the txt file

parquet_path = "/weka1/federico/the-stack/the-stack-v1_zstd.parquet"

if __name__ == "__main__":
    print(f"Starting at {time.asctime()}")
    txt_cont_path = f"/weka1/federico/the-stack/the-stack-v1-contents.txt"
    txt_cont_index = f"/weka1/federico/the-stack/the-stack-v1-contents-index.json"

    print(
        f"Reading from {parquet_path}, writing to {txt_cont_path} and {txt_cont_index}"
    )
    # build and write to file
    # txt_cont_path = (
    #     f"/weka1/federico/tests-tesi/mmap_vs_rdb/the-stack-{txt_size_str}-contents.txt"
    # )
    if not os.path.exists(txt_cont_path):
        # print(f"Building the file {txt_size_str}")
        print(f"Building the file {txt_cont_path}")
        sha_sizes = {}  # made of { sha: (start_index, size) }
        tot_put_time = 0
        with open(txt_cont_path, "a") as f:
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
                #     if tot_size >= txt_size:
                #         break
                # if tot_size >= txt_size:
                #     break
        # txt_cont_index = (
        #     f"/weka1/federico/tests-tesi/mmap_vs_rdb/txt-index_{txt_size_str}.json"
        # )
        # txt_cont_index = f"/weka1/federico/the-stack/the-stack-v1-contents-index.json"
        with open(txt_cont_index, "w") as f:
            f.write(json.dumps(sha_sizes, indent=4))
        print(
            f"Total time to write {round(tot_put_time, 3)} s, {round((tot_size / MiB) / tot_put_time, 3)} MiB/s"
        )
    else:
        print(f"File {txt_cont_path} already exists")
        # txt_cont_index = f"/weka1/federico/the-stack/the-stack-v1-contents-index.json"
        # txt_cont_index = (
        #     f"/weka1/federico/tests-tesi/mmap_vs_rdb/txt-index_{txt_size_str}.json"
        # )
        with open(txt_cont_index, "r") as f:
            sha_sizes = json.load(f)

    print(f"Half-time {time.asctime()}")

    # test get with mmap
    tot_get_time = 0
    tot_get_size = 0
    shas = list(sha_sizes.keys())
    if get_calls < len(shas):
        shas = shas[:get_calls]
    random.shuffle(shas)
    print(f"Testing the file retrieving {get_calls} contents")
    with open(txt_cont_path, "r") as f:
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

    print(f"Total time: {round(tot_get_time, 3)} s")
    print(f"Time per get: {tot_get_time / len(shas)} s")
    print(f"Throughput: {round((tot_get_size / MiB) / tot_get_time, 3)} MiB/s")

    print(f"Ending at {time.asctime()}")
