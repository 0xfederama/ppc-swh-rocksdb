import aimrocks

KiB = 1024
MiB = 1024 * 1024
GiB = 1024 * 1024 * 1024

parquet_path = "utils/the-stack-8M.parquet"
contents_path = "utils/the-stack-8M-contents.txt"
contents_index_path = "utils/the-stack-8M-contents-index.json"
rocksdb_output_path = "utils/"

# define orders
orders = [
    # "parquet",  # standard order of the parquet file (by language)
    # "rev-filename",
    # "ext-filename",
    "ext-filename-nopath",
    # "filename_repo",
    # "repo_filename",
    # "lang-ext-filename",
    # "tlsh",
]
# define compressors and block sizes
compressors = [
    # (aimrocks.CompressionType.no_compression, 0),
    # (aimrocks.CompressionType.zstd_compression, 3),
    # (aimrocks.CompressionType.zstd_compression, 12),
    # (aimrocks.CompressionType.zstd_compression, 22),
    (aimrocks.CompressionType.zlib_compression, 6),
    # (aimrocks.CompressionType.zlib_compression, 9),
    # (aimrocks.CompressionType.snappy_compression, 0),
]
block_sizes = [
    # 4 * KiB,
    # 8 * KiB,
    16 * KiB,
    # 32 * KiB,
    # 64 * KiB,
    # 128 * KiB,
    # 256 * KiB,
    # 512 * KiB,
    # 1 * MiB,
    # 4 * MiB,
    # 10 * MiB,
]
