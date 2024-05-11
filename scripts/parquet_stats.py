#!/usr/bin/env python3

import json
from operator import itemgetter
from pyarrow.parquet import ParquetFile

parquet_path = "/disk2/data/the-stack/the-stack-dedup_v1.parquet"

if __name__ == "__main__":
    pf = ParquetFile(parquet_path)

    languages = {}
    tot_size = 0
    for batch in pf.iter_batches(columns=["lang", "size"]):
        for i, lang in enumerate(batch["lang"]):
            lang = str(lang)
            cont_size = int(str(batch["size"][i]))
            languages[lang] = languages.get(lang, 0) + cont_size
            tot_size += cont_size

    sorted_lang = {
        k: v for k, v in sorted(languages.items(), key=itemgetter(1), reverse=True)
    }
    print(json.dumps(sorted_lang, indent=4))
    print("\nTotal size", tot_size / 1024 / 1024)
    print()

    for idx, k in enumerate(sorted_lang):
        if idx == 25:
            break
        print(f"{k}: {str(round(sorted_lang[k] / (1024 * 1024), 3))} MiB")
