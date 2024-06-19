from pyarrow.parquet import ParquetFile
import pandas as pd
import time
import os

parq_size = "10G"  # 5rec, 1M, 8M, 64M, 256M, 1G, 4G, 10G, 200G, dedup_v1, 1G_minsize_4M, 2G_minsize_1M, 10G_minsize_1012K, 24G_minsize_990K
small_parq_path = "/disk2/federico/the-stack/small/the-stack-" + parq_size + ".parquet"
full_parq_path = "/disk2/federico/the-stack/the-stack-" + parq_size + ".parquet"
parq_path = small_parq_path if "dedup_v1" not in parq_size else full_parq_path
blobs_path = f"/disk2/federico/blobs/{parq_size}"

KiB = 1024
MiB = 1024 * 1024
GiB = 1024 * 1024 * 1024

if __name__ == "__main__":
    print(f"Starting at {time.asctime()}, pid: {os.getpid()}")

    # create blobs directory. if it exists and is not empty, crash
    try:
        os.makedirs(blobs_path)
    except FileExistsError:
        if len(os.listdir(blobs_path)) != 0:
            print(f"Blobs directory {blobs_path} must be empty or non existent")
            exit()

    pf = ParquetFile(parq_path)
    start = time.time()
    rows = []  # list of dictionary rows
    for batch in pf.iter_batches(
        columns=[
            "hexsha",
            "content",
            "max_stars_repo_path",
            "max_stars_repo_name",
            "size",
        ]
    ):
        for i, sha in enumerate(batch["hexsha"]):
            sha = str(sha)
            content = str(batch["content"][i])
            filename = str(batch["max_stars_repo_path"][i])
            repo = str(batch["max_stars_repo_name"][i])
            size = int(str(batch["size"][i]))
            local_path = f"{sha[0:2]}/{sha[2:4]}"
            # create the csv row
            new_row = {
                "swh_id": 0,
                "file_id": sha,
                "length": size,
                "filename": filename,
                "filepath": os.path.join(repo, filename),
                "local_path": local_path,  # FIXME: this should be the path to the blob with the directories of the hash
            }
            rows.append(new_row)

            # create the file in the directory
            try:
                os.makedirs(os.path.join(blobs_path, local_path))
            except FileExistsError:
                # if the dir already exists, simply create the file in it
                pass
            with open(os.path.join(blobs_path, local_path, sha), "w") as f:
                f.write(content)

    df = pd.DataFrame(rows)
    df.to_csv(f"/disk2/federico/blobs/{parq_size}_list_of_files.csv", index=False)

    print(f"Ending at {time.asctime()}")
