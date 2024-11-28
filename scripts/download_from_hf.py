from datasets import load_dataset

parquet_output_path = "/path/to/file.parquet"

dataset = load_dataset("bigcode/the-stack-dedup", split="train", num_proc=64)

dataset.to_parquet(parquet_output_path)
