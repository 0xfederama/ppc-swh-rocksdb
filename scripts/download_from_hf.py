from datasets import load_dataset

cache_dir = "/path/to/cache/dir/"
parquet_output_path = "/path/to/file.parquet"

dataset = load_dataset(
    "bigcode/the-stack-dedup", split="train", cache_dir=cache_dir, num_proc=64
)

dataset.to_parquet(parquet_output_path)
