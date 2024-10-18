import itertools
import os
import time

import matplotlib.pyplot as plt
import matplotlib.patheffects as pe
from pyarrow.parquet import ParquetFile

parq_size = "dedup_v1"  # 5rec, 1M, 8M, 64M, 256M, 1G, 4G, 10G, 200G, dedup_v1, 1G_minsize_4M, 2G_minsize_1M, 10G_minsize_1012K, 24G_minsize_990K
small_parq_path = "/weka1/federico/the-stack/small/the-stack-" + parq_size + ".parquet"
full_parq_path = "/weka1/federico/the-stack/the-stack-" + parq_size + "-zstd.parquet"
parquet_path = small_parq_path if "dedup_v1" not in parq_size else full_parq_path
charts_dir = "charts"
print_bigfiles = True
info = ""
KiB = 1024
MiB = 1024 * 1024
GiB = 1024 * 1024 * 1024


def build_chart(x: list, y: list, xlab: str, ylab: str, name: str, loglog=False):
    plt.figure(figsize=(16, 9))
    # plot vertical lines
    if not loglog:
        vlines = [4, 256]
        for vline in vlines:
            if max(x) >= vline:
                plt.axvline(x=vline, color="grey", linestyle="--")
                try:
                    index = x.index(vline)
                    inters_y = y[index]
                    inters_y_str = inters_y
                    if "sizes" in name:
                        if "sum" in name:
                            inters_y_str = str(round(inters_y * GiB / GiB, 2)) + " GiB"
                        else:
                            inters_y_str = str(round(inters_y * MiB / MiB, 2)) + " MiB"
                    if "files" in name and "sum" in name:
                        inters_y_str = str(round(inters_y / (10**8), 1)) + " x 10^8"
                    plt.text(
                        vline,
                        inters_y,
                        f"({int(vline)} KiB, {inters_y_str})",
                        ha="center",
                        va="bottom",
                        color="white",
                        fontsize="11",
                        weight="bold",
                        path_effects=[pe.withStroke(linewidth=4, foreground="red")],
                    )
                    print(f"Vertical line at {vline} meets plot at {inters_y}")
                except ValueError:
                    print(f"Value error for vline {vline}")
    # manage logscale
    if loglog:
        plt.yscale("log")
        plt.xscale("log")
        y = sorted(y, reverse=True)
        # import matplotlib.ticker as ticker
        # ax = plt.gca()
        # ax.yaxis.set_major_formatter(
        #     ticker.LogFormatterExponent(base=10.0, labelOnlyBase=True)
        # )
    else:
        plt.xlabel(xlab)
        if name == "files":
            scilimits = (3, 3)
            plt.ticklabel_format(axis="y", style="sci", scilimits=scilimits)
        else:
            plt.ticklabel_format(style="plain", useOffset=False, axis="y")
    # draw the plot
    plt.ylabel(ylab)
    plt.step(x, y, where="post")
    plt.fill_between(x, y, step="post")
    plt.ylim(bottom=0)
    plt.savefig(f"{charts_dir}/{name}.png", format="png", bbox_inches="tight", dpi=120)
    plt.close()
    print(f"Graph {name} created\n")


if __name__ == "__main__":
    print(f"Starting at {time.asctime()}, pid: {os.getpid()}")
    print(f"Parquet: {parquet_path}\n")

    pf = ParquetFile(parquet_path)
    lang_sizes_tot = {}
    lang_files = {}
    lang_all_sizes = {}
    lang_maxsizes = {}
    tot_size = 0
    max_size = 0
    max_size_lang = None
    n_files = 0
    outliers = {  # pairs of num_files, size
        32: [0, 0],
        64: [0, 0],
        128: [0, 0],
        256: [0, 0],
        512: [0, 0],
        1024: [0, 0],
    }
    files_count_32 = {}
    files_count = {}
    sizes_count = {}
    columns = ["lang", "size"]
    if print_bigfiles:
        columns.append("content")
        columns.append("max_stars_repo_path")
        columns.append("max_stars_repo_name")
    for batch in pf.iter_batches(columns=columns):
        for i, lang in enumerate(batch["lang"]):
            lang = str(lang)
            size = int(str(batch["size"][i]))
            lang_sizes_tot[lang] = lang_sizes_tot.get(lang, 0) + size
            lang_files[lang] = lang_files.get(lang, 0) + 1
            langlist = []
            size_kb = round(size / KiB, 3)
            if lang_all_sizes.get(lang) is None:
                lang_all_sizes[lang] = []
            lang_all_sizes[lang].append(size_kb)
            if size > max_size:
                max_size = size
                max_size_lang = lang
            if size > lang_maxsizes.get(lang, 0):
                lang_maxsizes[lang] = size
            n_files += 1
            tot_size += size
            sizes = [32, 64, 128, 256, 512, 1024]
            for s in sizes:
                if size > s * KiB:
                    outliers[s][0] += 1
                    outliers[s][1] += size
            if size <= 1 * MiB:
                files_count[size] = files_count.get(size, 0) + 1
                sizes_count[size] = sizes_count.get(size, 0) + size
                if size <= 32 * KiB:
                    files_count_32[size] = files_count_32.get(size, 0) + 1
            if print_bigfiles:
                if (
                    (lang == "JavaScript" and size > 11 * MiB)
                    or (lang == "C" and size > 29 * MiB)
                    or (lang == "C++" and size > 10 * MiB)
                ):
                    content = str(batch["content"][i])
                    filename = str(batch["max_stars_repo_path"][i])
                    repo = str(batch["max_stars_repo_name"][i])
                    print(f"File {filename}, repo {repo}, size {size / MiB} MB")
                    if "/" in filename:
                        filename = filename.split("/")[-1]
                    with open(filename, "w") as f:
                        f.write(content)
                    print(f"Written content to file {filename}, size {size / MiB} MB")

    # sort the dictionaries
    files_count = dict(sorted(files_count.items()))
    sizes_count = dict(sorted(sizes_count.items()))
    files_count_32 = dict(sorted(files_count_32.items()))

    # get the axis lists
    x_axis = list(files_count.keys())
    y_files = list(files_count.values())
    y_sizes = list(sizes_count.values())
    x_axis_32 = list(files_count_32.keys())
    y_files_32 = list(files_count_32.values())

    x_axis_kb_32 = list(map(lambda n: round(n / KiB, 3), x_axis_32))
    x_axis_kb = list(map(lambda n: round(n / KiB, 3), x_axis))
    y_sizes_mb = list(map(lambda n: round(n / MiB, 6), y_sizes))

    # get the axis with the cumulative values
    y_files_sum = list(itertools.accumulate(y_files))
    y_sizes_sum = list(itertools.accumulate(y_sizes))
    y_sizes_sum_gb = list(map(lambda n: round(n / GiB, 9), y_sizes_sum))

    print(f"Total size: {round(tot_size / GiB, 3)} GiB")
    print(f"Max file size: {round(max_size / MiB, 3)} MiB, language {max_size_lang}")
    print(f"Average file size: {round((tot_size / KiB) / n_files, 3)} KiB")
    print(f"Number of files: {n_files}")
    print(
        f"Outliers > 32 KiB: {outliers[32][0]} ({round(outliers[32][0] * 100 / n_files, 3)} %), {round(outliers[32][1] / MiB, 3)} MiB ({round(outliers[32][1] * 100 / tot_size, 3)} %)"
    )
    print(
        f"Outliers > 64 KiB: {outliers[64][0]} ({round(outliers[64][0] * 100 / n_files, 3)} %), {round(outliers[64][1] / MiB, 3)} MiB ({round(outliers[64][1] * 100 / tot_size, 3)} %)"
    )
    print(
        f"Outliers > 128 KiB: {outliers[128][0]} ({round(outliers[128][0] * 100 / n_files, 3)} %), {round(outliers[128][1] / MiB, 3)} MiB ({round(outliers[128][1] * 100 / tot_size, 3)} %)"
    )
    print(
        f"Outliers > 256 KiB: {outliers[256][0]} ({round(outliers[256][0] * 100 / n_files, 3)} %), {round(outliers[256][1] / MiB, 3)} MiB ({round(outliers[256][1] * 100 / tot_size, 3)} %)"
    )
    print(
        f"Outliers > 512 KiB: {outliers[512][0]} ({round(outliers[512][0] * 100 / n_files, 3)} %), {round(outliers[512][1] / MiB, 3)} MiB ({round(outliers[512][1] * 100 / tot_size, 3)} %)"
    )
    print(
        f"Outliers > 1 MiB: {outliers[1024][0]} ({round(outliers[1024][0] * 100 / n_files, 5)} %), {round(outliers[1024][1] / MiB, 3)} MiB ({round(outliers[1024][1] * 100 / tot_size, 3)} %)"
    )
    print()

    os.mkdir(charts_dir)
    with open(f"{charts_dir}/info.txt", "w") as f:
        f.write(f"{parquet_path}\n{info}")

    # build files to 32K
    build_chart(
        x_axis_kb_32,
        y_files_32,
        "Size in KiB",
        "Number of files having a given size",
        "files",
    )

    # build files loglog 1M
    build_chart(
        x_axis,
        y_files,
        "Log size in KiB",
        "Log number of files having a given size",
        "files_log_log_1M",
        loglog=True,
    )

    # build cumulative files to 1M
    # build_chart(
    #     x_axis_kb,
    #     y_files_sum,
    #     "Size in KiB",
    #     "Number of files having at most a given size",
    #     "files_sum",
    # )

    # build sizes to 1M
    build_chart(
        x_axis_kb,
        y_sizes_mb,
        "Size in KiB",
        "Space in MiB of files having a given size",
        "sizes",
    )

    # build cumulative sizes to 1M
    build_chart(
        x_axis_kb,
        y_sizes_sum_gb,
        "Size in KiB",
        "Space in GiB of files having at most a given size",
        "sizes_sum",
    )

    # print top x languages
    from operator import itemgetter

    sorted_lang_sizes = {
        k: v for k, v in sorted(lang_sizes_tot.items(), key=itemgetter(1), reverse=True)
    }
    i = 0
    top = 20
    smalllang = {}
    biglang = {}
    print(f"Top {top} languages (out of {len(sorted_lang_sizes)}):")
    for lang, _ in sorted_lang_sizes.items():
        if i == top:
            break
        nfiles = lang_files[lang]
        langsize_gb = round(lang_sizes_tot[lang] / GiB, 3)
        avg_kb = round(lang_sizes_tot[lang] / lang_files[lang] / KiB, 3)
        max_kb = round(lang_maxsizes[lang] / MiB, 3)
        print(
            f"{lang}: {nfiles} files, {langsize_gb} GiB, {avg_kb} KiB avg size, {max_kb} MiB max size"
        )
        i += 1
        biglangs = ["Jupyter Notebook", "CSV"]  # , "JSON"]
        if lang not in biglangs:
            smalllang[lang] = lang_all_sizes[lang]
        else:
            biglang[lang] = lang_all_sizes[lang]

    name = "lang_boxplot_small"
    x = list(smalllang.keys())
    y = list(smalllang.values())
    plt.figure(figsize=(16, 9))
    plt.ylabel("File sizes in KiB")
    plt.xlabel("Languages")
    plt.xticks(rotation=45, ha="right")
    plt.boxplot(y, labels=x, patch_artist=True, showfliers=False)
    plt.savefig(f"{charts_dir}/{name}.png", format="png", bbox_inches="tight", dpi=120)
    plt.close()
    print(f"\nGraph {name} created")

    name = "lang_boxplot_big"
    if biglangs != {}:
        x = list(biglang.keys())
        y = list(biglang.values())
        plt.figure(figsize=(16, 9))
        plt.ylabel("File sizes in KiB")
        plt.xlabel("Languages")
        plt.xticks(rotation=45, ha="right")
        plt.boxplot(y, labels=x, patch_artist=True, showfliers=False)
        plt.savefig(
            f"{charts_dir}/{name}.png", format="png", bbox_inches="tight", dpi=120
        )
        plt.close()
        print(f"\nGraph {name} created")

    print(f"\nEnding at {time.asctime()}")
