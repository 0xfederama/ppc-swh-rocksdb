# PPC The Stack into RocksDB

In this repository, we explore Permute Partition Compress (PPC) techniques on source-code archives to employ the solution on the Software Heritage archive (SWH). Our focus is on indexing and compressing source code files from various code repositories into RocksDB. For our tests, we utilized a source-code dataset hosted by Hugging Face at [the-stack-v1-dedup](https://huggingface.co/datasets/bigcode/the-stack-dedup), permuted the data using different heuristics, and created multiple RocksDB databases to assess their differences and performance.

The base of the benchmark, shared between the two benchmarks `benchmark-not_sorted.py` and `benchmark-pre_sorted.py`, is the following: we first process the parquet file, apply different file-similarity-based sorting functions to create specific keys for the database. Then, we create key-value pairs with the newly created key and the file as the value, and insert them into RocksDB. Altough, there are some differences between the two benchmarks:
- `benchmark-not_sorted.py` doesn't pre-sort the keys before inserting into the database: we read the parquet and, for each row, we create the key-value pair and write it directly into RocksDB, relying on the storage engine to maintain key order. We don't need additional files to execute this benchmark.
- `benchmark-pre_sorted.py` pre-sorts the keys before insertion: we read the parquet file into a pandas dataframe, we sort it and we later insert the key-value pairs in RocksDB. In this way, we force the order on RocksDB by giving it pre-ordered data. Given the size of the datasets, we cannot sort the entire dataframe in memory, and we need to rely on mmap to read the file contents from another file. For this purpose, as we specify in the "Setup additional files" section of this README, we use `create_contents.py`.

### Repository structure

The repository is structured as follows:
- `benchmark-pre_sorted.py` and `benchmark-not_sorted.py`: benchmarks scripts written in Python.
- `config.py`: configuration options to be used in the benchmarks: block size, compressor, order and file paths.
- `create_contents.py`: support script to create auxiliary files needed by the experiments
- `scripts/`: contains smaller testing scripts we used during development, and the script `download_from_hf.py` to download the dataset from HuggingFace.
- `data/`: contains an example parquet (for a toy execution of our benchmarks) and you can use this directory for testing purposes, as we will see later.
- `example/`: contains an example library, with a `lib.py` file implementing a library for `benchmark-not_sorted.py` and a `main.py` script using it.

We executed our entire codebase using Python 3.11.9; a minimum of 3.10 is required.

### Sorting options
In our benchmarks, we considered the following content-similarity-based sorting options:
- `parquet`: no sorting; retains the natural file order of the input dataset.
- `rev-filename`: uses the reversed filename as the sorting key. For example, `path/to/file.ext` becomes `txe.elif/ot/htap`.
- `ext-filename`: sorts by file extension, followed by the filename, and the path in reverse. For example, `path/to/file.ext` becomes `ext.file/ot/htap`.
- `ext-filename-nopath`: sorts by file extension followed by the filename. For example, `path/to/file.ext` becomes `ext.file`.
- `lang-ext-filename`: uses the programming language, followed by the filename (e.g., `python-py.main`).
- `filename_repo`: sorts by filename (reversed) and then by repository if filenames are identical.
- `repo_filename`: sorts by repository and then by reversed filename.
- `tlsh`: sorts by TLSH fingerprint (locality-sensitive hashing).

Every order, after the options already seen, appends to the key the file size and the sha of the file, to order files by size and avoid duplicate keys.

## Development environment setup

In order to download the repo and setup the virtual environment, you need to run these commands:
```bash
git clone https://github.com/0xfederama/ppc-swh-rocksdb
cd ppc-swh-rocksdb
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

This repository includes a small toy dataset located in the `data` folder, for fast experiments. The default testing dataset is `data/the-stack-64M.parquet`, as specified in `config.py`, and you can skip the "Download the datasets" section.

### Download the datasets
To download a ready-made dataset like [the-stack-v1-dedup](https://huggingface.co/datasets/bigcode/the-stack-dedup), follow Hugging Face's guide to install the dataset and export it to Parquet. In `scripts/download_from_hf.py`, you'll find code to download The Stack dedup dataset; customize the paths and run the code. Any parquet file containing at least the columns `["hexsha", "max_stars_repo_path", "max_stars_repo_name", "content", "size", "lang"]` is eligible for testing with this repository.

In order to execute a toy experiment with our codebase, we already provide a pre-downloaded Parquet file, `data/the-stack-64M.parquet`, comprising a subset the first 64 MiB from The Stack v1 dedup.

### Setup additional files
If you aim to only run `benchmark-not_sorted.py`, you can skip this part.

To run `benchmark-pre_sorted.py`, you need to create the auxiliary files `contents.txt` and `contents-index.json`. The former contains the dataset's file contents, while the latter serves as an index with starting and ending positions for accessing specific file contents. These files, which essentially replicate the dataset, are utilized within `benchmark-pre_sorted.py`, where sorting the entire DataFrame (including contents) is not feasible. You can create these files by running `python3 create_contents.py`, modifying the paths in the initial lines of the script as needed.

## Run the benchmark

Both benchmarks, `benchmark-pre_sorted.py` and `benchmark-not_sorted.py`, read configurations from `config.py`, which you can modify to specify the datasets to test, output paths for the RocksDB database, and benchmark execution parameters (block size, compressor, ordering heuristic). The default settings in `config.py` include a block size of 16 kB, zlib-6 as the compressor, and `ext-filename-nopath` as the file reordering method, as these settings yielded competitive results in our research. After configuring `config.py`, run:

```bash
python3 benchmark-not_sorted.py
python3 benchmark-pre_sorted.py
```

We recommend using the `nohup` command to run tests in the background due to the long execution times required for larger datasets. For example:

```bash
nohup python3 benchmark-not_sorted.py &
```