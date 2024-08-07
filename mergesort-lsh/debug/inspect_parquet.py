import pyarrow.parquet as pq
import pandas as pd

# Path to the Parquet file
file_paths = [
  '/disk2/tosoni/test/sorted-1M.parquet',
  '/disk2/tosoni/test/sorted-4M.parquet',
  '/disk2/tosoni/test/sorted-8M.parquet',
  #'the-stack-4M.sorted.parquet',
  #'run-8.parquet',
  #'sorted.parquet',
]

baserow = 500
delta = 10

for file_path in file_paths :
    print(f'**********Processing file {file_path}')
    # Carica il dataframe da un file Parquet
    df = pd.read_parquet(file_path)

    # Get the number of rows and columns
    nrows = len(df)
    ncols = len(df.columns)
    print(f"shape: ({nrows}, {ncols})")

    # Itera sulle righe 200-300
    for index, row in df.iloc[baserow:baserow+delta].iterrows():
        # Fai qualcosa con ogni riga
        print(row['max_stars_repo_path'][:50].replace('\n', '@'))



