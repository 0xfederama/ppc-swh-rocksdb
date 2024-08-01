import pyarrow.parquet as pq
import pandas as pd

# Path to the input and output Parquet files
input_file_path = '/disk2/federico/the-stack/small/the-stack-4M.parquet'
suflen = len('.parquet')
prefix = input_file_path[:-suflen]
output_file_path = 'the-stack-4M.sorted.parquet' #f'{prefix}.sorted.parquet'

# Column to sort by
sort_column = 'max_stars_repo_path'

# Read the Parquet file into a pandas DataFrame
df = pq.read_table(input_file_path).to_pandas()

# Sort the DataFrame based on the specified column
df_sorted = df.sort_values(by=sort_column)

# Write the sorted DataFrame back to a Parquet file
df_sorted.to_parquet(output_file_path, index=False)

print(df.shape[0], 'rows sorted and written to', output_file_path)