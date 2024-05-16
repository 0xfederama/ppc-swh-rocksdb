#!/usr/bin/env python3

import itertools
import os
import time
from pyarrow.parquet import ParquetFile
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

parquet_path = "/disk2/data/the-stack/the-stack-dedup_v1.parquet"
# parquet_path = "/disk2/federico/the-stack/the-stack-small_10G.parquet"
KiB = 1024
MiB = 1024 * 1024
GiB = 1024 * 1024 * 1024
outliers_threshold = 32 * KiB


def build_graph(xaxis, yaxis, xlabel, ylabel, name):
    plt.figure(figsize=(16, 9))
    plt.axvline(x=4, color="grey", linestyle="--")
    # plt.axvline(x=256, color="grey", linestyle="--")
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    ax = plt.gca()
    if "log" in name:
        plt.yscale("log")
        ax.yaxis.set_major_formatter(
            mtick.LogFormatterExponent(base=10.0, labelOnlyBase=True)
        )
    else:
        plt.ticklabel_format(axis="y", style="sci", scilimits=(0, 0))
    plt.bar(xaxis, yaxis)
    plt.savefig(f"stats_graphs/{name}.png", format="png")
    plt.close()
    print(f"Graph {name} created")


if __name__ == "__main__":
    print(f"Starting at {time.asctime()}, pid: {os.getpid()}\n")

    start = time.time()
    pf = ParquetFile(parquet_path)
    lang_sizes = {}
    lang_files = {}
    tot_size = 0
    max_size = 0
    n_files = 0
    outliers = 0
    sizes_count = {}
    files_count = {}
    for batch in pf.iter_batches(columns=["lang", "size"]):
        for i, lang in enumerate(batch["lang"]):
            lang = str(lang)
            size = int(str(batch["size"][i]))
            lang_sizes[lang] = lang_sizes.get(lang, 0) + size
            lang_files[lang] = lang_files.get(lang, 0) + 1
            max_size = max(max_size, size)
            n_files += 1
            tot_size += size
            size_kb = round(size / KiB, 3)
            if size <= outliers_threshold:
                files_count[size_kb] = files_count.get(size_kb, 0) + 1
                sizes_count[size_kb] = sizes_count.get(size_kb, 0) + size_kb
            else:
                outliers += 1

    end = time.time()
    print(f"Read file: {round(end-start)} s")
    start = time.time()

    files_count = dict(sorted(files_count.items()))
    sizes_count = dict(sorted(sizes_count.items()))
    x_axis = list(files_count.keys())  # same between files and sizes
    y_files = files_count.values()
    y_sizes = sizes_count.values()
    y_files_sum = list(itertools.accumulate(y_files))
    y_sizes_sum = list(itertools.accumulate(y_sizes))

    end = time.time()
    print(f"Sort and accumulate: {round(end-start)} s\n")

    print(f"Total size: {round(tot_size / GiB, 3)} GiB")
    print(f"Max file size: {round(max_size / MiB, 3)} MiB")
    print(f"Average file size: {round((tot_size / KiB) / n_files, 3)} KiB")
    print(f"Number of files: {n_files}")
    print(
        f"Outliers > {round(outliers_threshold / KiB)} KiB: {outliers} ({round(outliers * 100 / n_files, 3)} %)"
    )
    print()

    # graph for files_count
    build_graph(x_axis, y_files, "Size in KiB", "Number of files = size", "files_count")

    # graph for log_files_count
    build_graph(
        x_axis,
        y_files,
        "Size in KiB",
        "Log of number of files = size",
        "files_count_log",
    )

    # graph for files_count
    build_graph(
        x_axis,
        y_files_sum,
        "Size in KiB",
        "Number of files <= size",
        "files_count_sum",
    )

    # graph for log_files_count
    build_graph(
        x_axis,
        y_files_sum,
        "Size in KiB",
        "Log of number of files <= size",
        "files_count_sum_log",
    )

    # graph for sizes_count
    build_graph(x_axis, y_sizes, "Size in KiB", "Tot file size = size", "sizes_count")

    # graph for log_sizes_count
    build_graph(
        x_axis, y_sizes, "Size in KiB", "Log of tot file size = size", "sizes_count_log"
    )

    # graph for sizes_count
    build_graph(
        x_axis, y_sizes_sum, "Size in KiB", "Tot file size <= size", "sizes_count_sum"
    )

    # graph for log_sizes_count
    build_graph(
        x_axis,
        y_sizes_sum,
        "Size in KiB",
        "Log of tot file size <= size",
        "sizes_count_sum_log",
    )

    print(f"Finished graphs, {time.asctime()}")
    # print top x languages
    from operator import itemgetter

    sorted_lang_files = {
        k: v for k, v in sorted(lang_files.items(), key=itemgetter(1), reverse=True)
    }
    i = 0
    top = 20
    print(f"\nTop {top} languages:")
    for lang, size in sorted_lang_files.items():
        if i == top:
            break
        print(
            f"{lang}: {str(round(lang_sizes[lang] / GiB, 3))} GiB, {str(lang_files[lang])} files"
        )
        i += 1

    print(f"\nEnding at {time.asctime()}")
