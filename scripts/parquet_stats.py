#!/usr/bin/env python3

import os
import time
from pyarrow.parquet import ParquetFile
import matplotlib.pyplot as plt

parquet_path = "/disk2/data/the-stack/the-stack-dedup_v1.parquet"
# parquet_path = "/disk2/federico/the-stack/the-stack-small_256M.parquet"
KiB = 1024
MiB = 1024 * 1024
GiB = 1024 * 1024 * 1024
outliers_threshold = 32 * KiB


def build_graph(res_dict, xlabel, ylabel, name):
    plt.figure(figsize=(16, 9))
    plt.axvline(x=4, color="grey", linestyle="--")
    # plt.axvline(x=256, color="grey", linestyle="--")
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    if name.startswith("log"):
        plt.yscale("log")
    plt.bar(list(res_dict.keys()), res_dict.values())
    plt.savefig(f"graphs/{name}.png", format="png", dpi=120)
    plt.close()


if __name__ == "__main__":
    print(f"Starting at {time.asctime()}, pid: {os.getpid()}")

    pf = ParquetFile(parquet_path)
    languages_sizes = {}
    languages_files = {}
    tot_size = 0
    max_size = 0
    n_files = 0
    outliers = 0
    files_count = {}
    sizes_count = {}
    sum_files_count = {}
    sum_sizes_count = {}
    for batch in pf.iter_batches(columns=["lang", "size"]):
        for i, lang in enumerate(batch["lang"]):
            lang = str(lang)
            cont_size = int(str(batch["size"][i]))
            languages_sizes[lang] = languages_sizes.get(lang, 0) + cont_size
            languages_files[lang] = languages_files.get(lang, 0) + 1
            max_size = max(max_size, cont_size)
            n_files += 1
            tot_size += cont_size
            # size_kb = round(cont_size / KiB, 5)
            size_kb = cont_size / KiB
            if cont_size <= outliers_threshold:
                files_count[size_kb] = files_count.get(size_kb, 0) + 1
                sizes_count[size_kb] = sizes_count.get(size_kb, 0) + size_kb
                sum_files = 0
                sum_sizes = 0
                for size, num in sum_sizes_count.items():
                    if size < size_kb:
                        sum_files += files_count[size]
                        sum_sizes += sizes_count[size]
                    if size > size_kb:
                        sum_files_count[size] += 1
                        sum_sizes_count[size] += size_kb
                if size_kb in sum_files_count:
                    sum_files_count[size_kb] += 1
                    sum_sizes_count[size_kb] += size_kb
                else:
                    sum_files_count[size_kb] = sum_files + 1
                    sum_sizes_count[size_kb] = sum_sizes + size_kb
            else:
                outliers += 1

    print(f"Total size: {round(tot_size / GiB, 3)} GiB")
    print(f"Max file size: {round(max_size / MiB, 3)} MiB")
    print(f"Average file size: {round((tot_size / KiB) / n_files, 3)} KiB")
    print(f"Number of files: {n_files}")
    print(
        f"Outliers > {round(outliers_threshold / KiB)} KiB: {outliers} ({round(outliers * 100 / n_files, 3)} %)"
    )

    # graph for files_count
    build_graph(files_count, "Size in KiB", "Number of files = size", "files_count")

    # graph for log_files_count
    build_graph(
        files_count, "Size in KiB", "Log of number of files = size", "log_files_count"
    )

    # graph for files_count
    build_graph(
        sum_files_count, "Size in KiB", "Number of files <= size", "sum_files_count"
    )

    # graph for log_files_count
    build_graph(
        sum_files_count,
        "Size in KiB",
        "Log of number of files <= size",
        "log_sum_files_count",
    )

    # graph for sizes_count
    build_graph(sizes_count, "Size in KiB", "Tot file size = size", "sizes_count")

    # graph for log_sizes_count
    build_graph(
        sizes_count, "Size in KiB", "Log of tot file size = size", "log_sizes_count"
    )

    # graph for sizes_count
    build_graph(
        sum_sizes_count, "Size in KiB", "Tot file size <= size", "sum_sizes_count"
    )

    # graph for log_sizes_count
    build_graph(
        sum_sizes_count,
        "Size in KiB",
        "Log of tot file size <= size",
        "log_sum_sizes_count",
    )

    # print top x languages
    from operator import itemgetter

    sorted_lang_files = {
        k: v
        for k, v in sorted(languages_files.items(), key=itemgetter(1), reverse=True)
    }
    i = 0
    for lang, size in sorted_lang_files.items():
        if i == 20:
            break
        print(
            f"{lang}: {str(round(languages_sizes[lang] / GiB, 3))} GiB, {str(languages_files[lang])} files"
        )

    print(f"Ending at {time.asctime()}")
