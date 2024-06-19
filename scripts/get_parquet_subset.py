import os
import time
import pandas as pd
from pyarrow.parquet import ParquetFile

size = float("inf")  # float('inf') to take all the files
size_str = ""  # automatically the ending size if size is inf
minsize = 0  # 0 to take files with all sizes
minsize_str = "1M"
languages = ["C", "C++"]  # [] to take all languages
lang_str = "cc"

KiB = 1024
MiB = 1024 * 1024
GiB = 1024 * 1024 * 1024

parq_path = "/disk2/data/the-stack/the-stack-dedup_v1.parquet"

if __name__ == "__main__":
    print(f"Starting at {time.asctime()}, pid: {os.getpid()}")
    print(
        f"Getting a subset of {size_str if size != float('inf') else 'inf'} from {parq_path} with min size {minsize_str if minsize != 0 else '0'}, languages {languages}"
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
            lang = str(row["lang"])
            if lang in languages or languages == []:
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

    output_path = "/disk2/federico/the-stack"
    if languages != []:
        output_path += "/langs"
        lang_str = "-" + lang_str
    else:
        lang_str = ""
    if minsize == 0:
        output_path += f"/the-stack-{size_str}{lang_str}.parquet"
    else:
        minsize_str = "_minsize_" + minsize_str
        output_path += f"/the-stack-{size_str}{minsize_str}{lang_str}.parquet"
    df.to_parquet(output_path, compression=None)

    end_time = time.time()
    tot_time = end_time - start_time

    print(f"Parquet {output_path} created in {round(tot_time)} s")
    print(f"Ending at {time.asctime()}")
