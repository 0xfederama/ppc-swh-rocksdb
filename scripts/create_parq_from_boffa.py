import os
import time

import pandas as pd

KiB = 1024
MiB = 1024 * 1024
GiB = 1024 * 1024 * 1024


def main():
    df = pd.read_csv("/disk2/data/200GiB/Python_selection/Python_selection_info.csv")
    rows = []

    # walk blobs directory
    blobs_dir = "/data/swh/blobs_uncompressed"
    size = 0
    for root, _, files in os.walk(blobs_dir):
        for file in files:
            # if file is in dataframe list
            if file in df["file_id"].values:
                # get row from df to get values
                row = df.loc[df["file_id"] == file].to_dict()
                # read blob content
                with open(os.path.join(root, file), "r") as f:
                    content = f.read()
                # append to rows
                rows.append(
                    {
                        "hexsha": row["hexsha"],
                        "max_stars_repo_path": row["max_stars_repo_path"],
                        "content": content,
                        "size": row["length"],
                        "lang": "Python",
                    },
                )
                size += row["length"]

    print(f"Read all blobs, creating parquet file, {time.asctime()}")
    print(f"Files: {len(rows)}")
    print(f"Total files size: {size / GiB} GiB")

    new_df = pd.DataFrame(rows)
    new_df.to_parquet("/weka1/federico/boffa-200G.parquet", compression=None)


if __name__ == "__main__":
    print(f"Starting at {time.asctime()}, pid: {os.getpid()}")
    main()
    print(f"Ending at {time.asctime()}")
