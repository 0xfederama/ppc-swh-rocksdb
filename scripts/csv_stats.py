import itertools
import os
import time

import matplotlib.pyplot as plt
import matplotlib.patheffects as pe

csv_path = "/weka1/federico/boffa-200G-py/Python_selection_info.csv"
charts_dir = "charts"
info = ""

KiB = 1024
MiB = 1024 * 1024
GiB = 1024 * 1024 * 1024

print_bigfiles = False


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
                except ValueError as e:
                    print(f"Value error for vline {vline}: {e}")
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
    print(f"Csv: {csv_path}\n")

    tot_size = 0
    max_size = 0
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
    bigfiles = 0
    smallfiles = 0
    with open(csv_path, "r") as f:
        next(f)
        for line in f:
            splitline = line.split(",")
            length = splitline[2]
            fname = splitline[5].strip()
            size = int(length)
            size_kb = round(size / KiB, 3)
            size_mb = round(size / MiB, 3)
            n_files += 1
            max_size = max(size, max_size)
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
            if size >= 30 * MiB:
                if print_bigfiles:
                    print(f"Size {size_mb} MiB: {fname}")
                bigfiles += 1

    if 4 * KiB not in files_count:
        files_count[4 * KiB] = 1
        sizes_count[4 * KiB] = 4 * KiB
        files_count_32[4 * KiB] = 1
    if 256 * KiB not in files_count:
        files_count[256 * KiB] = 1
        sizes_count[256 * KiB] = 256 * KiB

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
    print(f"Max file size: {round(max_size / KiB, 3)} MiB")
    print(f"Average file size: {round((tot_size / KiB) / n_files, 3)} KiB")
    print(f"Number of files: {n_files}")
    print(f"Files bigger than 30 MiB: {bigfiles}")
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
        f.write(f"{csv_path}\n{info}")

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

    print(f"\nEnding at {time.asctime()}")
