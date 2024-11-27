# PPC for the Software Heritage Archive (SWH) via RocksDB

In this repository, we explore Permute Partition Compress (PPC) techniques for the Software Heritage archive (SWH). Our focus is on indexing and compressing source code files from various code repositories into RocksDB. For our tests, we utilized Software Heritage code hosted by Hugging Face at [the-stack-v1-dedup](https://huggingface.co/datasets/bigcode/the-stack-dedup), permuted the data using different heuristics, and created multiple RocksDB databases to assess their differences and performance.

Specifically, we first process the parquet file (excluding file content) and generate a Pandas DataFrame. We then apply different file-similarity-based sorting functions. For each element in the sorted DataFrame, we retrieve the file content, assign a suitable key, and insert it into RocksDB. ![benchmark architecture][]

### Repository Structure
The repository is structured as follows:
- `benchmark-pre_sorted.py` and `benchmark-not_sorted.py`: Benchmark scripts written in Python.
- `create_contents.py`: Generates auxiliary files to set up the experiments.
- `scripts/`: Contains files for smaller tests used during development.
- `utils/`: Contains various utility scripts.

We executed our entire codebase using Python 3.11.9; a minimum of 3.10 is required.

## Documentation and Options
In `benchmark-pre_sorted.py`, we considered the following content-similarity-based sorting options:
- `parquet`: No sorting; retains the natural file order of the input dataset.
- `rev_filename`: Uses the reversed filename as the sorting key. For example, `path/to/file.ext` becomes `txe.elif/ot/htap`.
- `ext-filename`: Sorts by file extension, followed by the filename, and the path in reverse. For example, `path/to/file.ext` becomes `ext.file/ot/htap`.
- `ext-filename-nopath`: Sorts by file extension followed by the filename. For example, `path/to/file.ext` becomes `ext.file`.
- `lang-ext-filename`: Uses the programming language, followed by the filename and a file SHA at the end (e.g., `python-py.main-sha`).
- `filename_repo`: Sorts by filename (reversed) and then by repository if filenames are identical.
- `repo_filename`: Sorts by repository and then by reversed filename.
- `tlsh`: Sorts by TLSH fingerprint (locality-sensitive hashing) and by size if fingerprints match.

## Development Environment Setup

The root of this repository contains two benchmarking scripts:
- `benchmark-pre_sorted.py`: Sorts files before inserting them into RocksDB.
- `benchmark-not_sorted.py`: Inserts files into RocksDB in the order they appear in the parquet file, relying on the storage engine to maintain key order.

## Code Setup
To download the repository and set up the virtual environment, run the following commands:

```bash
git clone https://github.com/0xfederama/ppc-swh-rocksdb
cd ppc-swh-rocksdb
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

This repository includes eight small datasets for testing located in the `utils` folder. The default testing dataset is `utils/the-stack-8M.parquet`, as specified in `config.py`.

### Download the Datasets

To download a ready-made dataset like [the-stack-v1-dedup](https://huggingface.co/datasets/bigcode/the-stack-dedup), follow Hugging Face's guide to install the dataset and export it to Parquet. In `scripts/download_from_hf.py`, you'll find code to download The Stack dedup dataset; customize the paths and run the code. Any parquet file containing at least the columns `["hexsha", "max_stars_repo_path", "max_stars_repo_name", "content", "size", "lang"]` is eligible for testing with this repository. To run `benchmark-pre_sorted.py`, you need to create the auxiliary files `contents.txt` and `contents-index.json`. The former contains the dataset's file contents, while the latter serves as an index with starting and ending positions for accessing specific file contents. These files, which essentially replicate the dataset, are utilized within `benchmark-pre_sorted.py`, where sorting the entire DataFrame (including contents) is not feasible. You can create these files by running `python3 scripts/create_contents.py`, modifying the paths in the initial lines of the script as needed.

### Run the Benchmark

Both benchmarks, `benchmark-pre_sorted.py` and `benchmark-not_sorted.py`, read configurations from `config.py`, which you can modify to specify the datasets to test, output paths for the RocksDB database, and benchmark execution parameters (block size, compressor, ordering heuristic). The default settings in `config.py` include a block size of 16 kB, zlib-6 as the compressor, and `ext-filename-nopath` as the file reordering method, as these settings yielded competitive results in our research. After configuring `config.py`, run:

```bash
python3 benchmark-not_sorted.py
python3 benchmark-pre_sorted.py
```

We recommend using the `nohup` command to run tests in the background due to the long execution times required for larger datasets. For example:

```bash
nohup python3 benchmark-not_sorted.py &
```
