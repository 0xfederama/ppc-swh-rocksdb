#!/usr/bin/env python3

from operator import itemgetter
from pyarrow.parquet import ParquetFile
import matplotlib.pyplot as plt

parquet_path = "/disk2/data/the-stack/the-stack-dedup_v1.parquet"
KiB = 1024
MiB = 1024 * 1024
GiB = 1024 * 1024 * 1024
outliers_threshold = 256 * KiB


if __name__ == "__main__":
    pf = ParquetFile(parquet_path)

    languages = {}
    tot_size = 0
    max_size = 0
    n_files = 0
    outliers = 0
    sizes_count = {}
    for batch in pf.iter_batches(columns=["lang", "size"]):
        for i, lang in enumerate(batch["lang"]):
            lang = str(lang)
            cont_size = int(str(batch["size"][i]))
            languages[lang] = languages.get(lang, 0) + cont_size
            max_size = max(max_size, cont_size)
            n_files += 1
            tot_size += cont_size
            if cont_size > outliers_threshold:
                outliers += 1
                continue
            size_kb = round(cont_size / KiB, 1)

            # approximate the result
            # def custom_round(num, base):
            #     return round(num / base) * base
            # if cont_size > (8 * KiB):
            #     size_kb = round(cont_size / KiB)
            # elif cont_size > (4 * KiB):
            #     size_kb = round(custom_round(cont_size / KiB, 0.5), 1)
            #     # print(f"DBG 2 < s < 4: {size_kb}")
            # else:
            #     size_kb = round(custom_round(cont_size / KiB, 0.2), 1)
            #     # print(f"DBG s < 2: {size_kb}")
            sizes_count[size_kb] = sizes_count.get(size_kb, 0) + 1

    sorted_lang = {
        k: v for k, v in sorted(languages.items(), key=itemgetter(1), reverse=True)
    }

    print(f"Total size: {round(tot_size / GiB, 3)} GiB")
    print(f"Max file size: {round(max_size / MiB, 3)} MiB")
    print(f"Average file size: {round((tot_size / KiB) / n_files, 3)} KiB")
    print(f"Number of files: {n_files}")
    print(
        f"Outliers > {round(outliers_threshold / KiB)} KiB: {outliers} ({round(outliers * 100 / n_files, 3)} %)"
    )

    plt.figure(figsize=(16, 9))
    plt.axvline(x=4, color="grey", linestyle="--")
    plt.axvline(x=256, color="grey", linestyle="--")
    plt.xlabel("Size in KiB")
    plt.ylabel("Number of files of that size")
    plt.bar(list(sizes_count.keys()), sizes_count.values())
    plt.savefig("fig.png", format="png", dpi=120)

    # print top x languages
    # x = 10
    # for idx, k in enumerate(sorted_lang):
    #     if idx == x:
    #         break
    #     print(f"{k}: {str(round(sorted_lang[k] / MiB, 3))} MiB")
