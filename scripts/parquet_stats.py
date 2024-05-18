#!/usr/bin/env python3

import itertools
import os
import time

import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
from pyarrow.parquet import ParquetFile

# parquet_path = "/disk2/federico/the-stack/the-stack-small_10G.parquet"
parquet_path = "/disk2/data/the-stack/the-stack-dedup_v1.parquet"
charts_dir = "charts"
KiB = 1024
MiB = 1024 * 1024
GiB = 1024 * 1024 * 1024


def build_chart(x: list, y: list, xlab: str, ylab: str, name: str, logscale=False):
    plt.figure(figsize=(16, 9))
    # plot vertical lines
    vlines = [4, 256]
    for vline in vlines:
        if max(x) >= vline:
            plt.axvline(x=vline, color="grey", linestyle="--")
            try:
                index = x.index(vline)
                inters_y = y[index]
                if "sizes" in name:
                    inters_y = round(inters_y / MiB, 3)
                plt.text(
                    vline,
                    inters_y,
                    f"({int(vline)}, {inters_y:.3f})",
                    ha="center",
                    va="bottom",
                )
                print(f"Vertical line at {vline} meets plot at {inters_y}")
            except ValueError:
                print(f"Value error for vline {vline}")
    plt.xlabel(xlab)
    plt.ylabel(ylab)
    # set scale for y
    if logscale:
        ax = plt.gca()
        plt.yscale("log")
        ax.yaxis.set_major_formatter(
            mtick.LogFormatterExponent(base=10.0, labelOnlyBase=True)
        )
    else:
        scilimits = (0, 0)
        if "sizes" in name and "sum" in name:
            scilimits = (6, 6)
        plt.ticklabel_format(axis="y", style="sci", scilimits=scilimits)
    # draw the plot
    plt.step(x, y, where="post")
    plt.fill_between(x, y, step="post")
    plt.ylim(bottom=0)
    plt.savefig(f"{charts_dir}/{name}.png", format="png")
    plt.close()
    print(f"Graph {name} created")


if __name__ == "__main__":
    print(f"Starting at {time.asctime()}, pid: {os.getpid()}")
    print(f"Parquet: {parquet_path}\n")

    start = time.time()
    pf = ParquetFile(parquet_path)
    lang_sizes = {}
    lang_files = {}
    tot_size = 0
    max_size = 0
    n_files = 0
    outliers_32 = 0
    outliers_256 = 0
    outliers_32_size = 0
    outliers_256_size = 0
    files_count_32 = {}
    files_count_256 = {}
    sizes_count_256 = {}
    sizes_count_tot = {}
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
            sizes_count_tot[size_kb] = sizes_count_tot.get(size_kb, 0) + size_kb
            if size > 256 * KiB:
                outliers_32 += 1
                outliers_32_size += size
                outliers_256 += 1
                outliers_256_size += size
            else:
                files_count_256[size_kb] = files_count_256.get(size_kb, 0) + 1
                sizes_count_256[size_kb] = sizes_count_256.get(size_kb, 0) + size_kb
                if size > 32 * KiB:
                    outliers_32 += 1
                    outliers_32_size += size
                else:
                    files_count_32[size_kb] = files_count_32.get(size_kb, 0) + 1

    end = time.time()
    print(f"Read file: {round(end-start)} s")
    start = time.time()

    # sort the dictionaries
    files_count_32 = dict(sorted(files_count_32.items()))
    files_count_256 = dict(sorted(files_count_256.items()))
    sizes_count_256 = dict(sorted(sizes_count_256.items()))
    sizes_count_tot = dict(sorted(sizes_count_tot.items()))

    # get the axis lists
    x_axis_32 = list(files_count_32.keys())
    x_axis_256 = list(files_count_256.keys())
    x_axis_tot = list(sizes_count_tot.keys())

    y_files_32 = list(files_count_32.values())
    y_files_256 = list(files_count_256.values())
    y_sizes_256 = list(sizes_count_256.values())
    y_sizes_tot = list(sizes_count_tot.values())

    # get the axis with the cumulative values
    y_files_256_sum = list(itertools.accumulate(y_files_256))
    y_sizes_256_sum = list(itertools.accumulate(y_sizes_256))
    y_sizes_tot_sum = list(itertools.accumulate(y_sizes_tot))

    end = time.time()
    print(f"Sort and accumulate: {round(end-start)} s\n")

    print(f"Total size: {round(tot_size / GiB, 3)} GiB")
    print(f"Max file size: {round(max_size / MiB, 3)} MiB")
    print(f"Average file size: {round((tot_size / KiB) / n_files, 3)} KiB")
    print(f"Number of files: {n_files}")
    print(
        f"Outliers > 32 KiB: {outliers_32} ({round(outliers_32 * 100 / n_files, 3)} %), {round(outliers_32_size / MiB, 3)} MiB ({round(outliers_32_size * 100 / tot_size, 3)} %)"
    )
    print(
        f"Outliers > 256 KiB: {outliers_256} ({round(outliers_256 * 100 / n_files, 3)} %), {round(outliers_256_size / MiB, 3)} MiB ({round(outliers_256_size * 100 / tot_size, 3)} %)"
    )
    print()

    os.mkdir(charts_dir)
    with open(f"{charts_dir}/info.txt", "w") as f:
        info = ""
        f.write(f"{parquet_path}\n{info}")

    # graph for files_count
    build_chart(
        x_axis_32,
        y_files_32,
        "Size in KiB",
        "Number of files whose size is equal to x",
        "files",
    )

    # graph for log_files_count
    # build_chart(
    #     x_axis_256,
    #     y_files_256,
    #     "Size in KiB",
    #     "Log of number of files = size",
    #     "files_count_log",
    #     logscale=True,
    # )

    # graph for sum_files_count
    build_chart(
        x_axis_256,
        y_files_256_sum,
        "Size in KiB",
        "Number of files whose size is less than or equal to x",
        "files_sum",
    )

    # graph for sizes_count
    build_chart(
        x_axis_256,
        y_sizes_256,
        "Size in KiB",
        "Total file size of files whose size is equal to x",
        "sizes",
    )
    build_chart(
        x_axis_tot,
        y_sizes_tot,
        "Size in KiB",
        "Total file size of files whose size is equal to x",
        "sizes_tot",
    )

    # graph for sum_sizes_count
    build_chart(
        x_axis_256,
        y_sizes_256_sum,
        "Size in KiB",
        "Total file size of files whose size is less than or equal to x",
        "sizes_sum",
    )
    build_chart(
        x_axis_tot,
        y_sizes_tot_sum,
        "Size in KiB",
        "Total file size of files whose size is less than or equal to x",
        "sizes_tot_sum",
    )

    print(f"\nFinished graphs, {time.asctime()}")

    # print top x languages
    from operator import itemgetter

    sorted_lang_sizes = {
        k: v for k, v in sorted(lang_sizes.items(), key=itemgetter(1), reverse=True)
    }
    i = 0
    top = 20
    print(f"\nTop {top} languages:")
    for lang, size in sorted_lang_sizes.items():
        if i == top:
            break
        print(
            f"{lang}: {str(lang_files[lang])} files, {str(round(lang_sizes[lang] / GiB, 3))} GiB, {str(round(lang_sizes[lang] / lang_files[lang] / KiB, 3))} KiB avg size"
        )
        i += 1

    print(f"\nEnding at {time.asctime()}")
