import os
import time

import pandas as pd

KiB = 1024
MiB = 1024 * 1024
GiB = 1024 * 1024 * 1024


def main():
    csv_file = "/weka1/federico/boffa-200G-py/Python_selection_info.csv"
    # csv_file = "/disk2/data/25GiB/Python_selection/Python_selection_info.csv"
    # csv_file = "/weka1/federico/boffa-debug/0G/Python_small.csv"

    blobs_dir = "/data/swh/blobs_uncompressed/"

    print(f"Reading from csv {csv_file}, blobs from {blobs_dir}")

    df = pd.read_csv(csv_file)
    print(f"CSV has been read, {time.asctime()}")

    size_boffa = 0
    size_measured = 0
    print_every_xgb = 1
    new_rows = []
    for index, row in df.iterrows():
        # get content of file
        file_id = row["file_id"]
        path_on_disk = os.path.join(blobs_dir, file_id[:2], file_id)
        try:
            with open(path_on_disk, "rb") as f:
                content = f.read()
        except Exception as e:
            print(f"File at index {index}: {path_on_disk}, exception {e}")
            exit()

        # insert row
        length = int(row["length"])
        filepath = row["filepath"]
        new_row = {
            "hexsha": file_id,
            "max_stars_repo_path": filepath,
            "content": content,
            "size": length,
            "lang": "Python",
        }
        new_rows.append(new_row)
        size_boffa += length
        size_measured += len(content)
        if size_measured > print_every_xgb * 10 * GiB:
            print(f"Currently {round(size_measured / GiB, 5)} GiB")
            print_every_xgb += 1

    print(f"Read all blobs, creating parquet file, {time.asctime()}")
    print(f"Files: {len(new_rows)}")
    print(
        f"Total files size: {round(size_boffa / GiB, 5)} GiB (boffa), {round(size_measured / GiB, 5)} GiB (measured)"
    )
    print(f"Average file size: {round(size_measured / len(new_rows) / KiB, 3)} KiB")

    new_df = pd.DataFrame(new_rows)
    new_df.to_parquet(
        "/weka1/federico/boffa-200G-py/dataset.parquet",
        # "/weka1/federico/boffa-debug/25G/dataset.parquet",
        # "/weka1/federico/boffa-debug/0G/dataset.parquet",
        compression=None,
    )


if __name__ == "__main__":
    print(f"Starting at {time.asctime()}, pid: {os.getpid()}")
    main()
    print(f"Ending at {time.asctime()}")
