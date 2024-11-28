import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import heapq
import os
import sys

class BatchedRunWriter:
    schema = None
    pw = None
    buffer = dict()
    batch_size = 100000

    def __init__(self, outpath, schema, batch_size, compression=None): 
        # Initialize the BatchedRunWriter with output path, schema, batch size, and compression
        self.schema = schema
        self.pw = pq.ParquetWriter(outpath, schema, compression=compression)
        self.buffer = {field:[] for field in schema}
        self.batch_size = batch_size

    def flush(self):
        # Write the buffered data to the Parquet file and clear the buffer
        if self.buffer:
            table = pa.Table.from_arrays([pa.array(col) for _, col in self.buffer.items()], schema=self.schema)
            self.pw.write_table(table)
            for col in self.buffer.values():
                col.clear()
                        
    def write(self, row):
        # Add a row to the buffer and flush if the buffer reaches the batch size
        if len(row) != len(self.buffer):
            print('row:', row, file=sys.stderr)
            print('buffer:', self.buffer, file=sys.stderr)
            raise ValueError("Row length does not match buffer length")
        for val, (_, col) in zip(row, self.buffer.items()):
            col.append(val)
        if len(col) == self.batch_size:
            self.flush()

    def close(self):
        # Flush remaining data and close the Parquet writer
        self.flush()
        self.pw.close()

def reverse_path(input_str):
    # Reverse the path in the input string and change the extension to the front
    dot_pos = input_str.rfind('.')
    extension = input_str[dot_pos+1:]
    path_parts = input_str[:dot_pos].split('/')
    reversed_path_parts = path_parts[::-1]
    output_str = f"{extension}.{'/'.join(reversed_path_parts)}"
    return output_str

def gen_parquet_lines(parquet_path):
    # Generate rows from a Parquet file
    debug = 0
    parquet_file = pq.ParquetFile(parquet_path)
    for batch in parquet_file.iter_batches():
        batch_df = batch.to_pandas()
        for _, row in batch_df.iterrows():
            yield row
            debug += 1
    print(parquet_path, ':', debug, file=sys.stderr)
    yield None

def merge_sort(nruns, batch_size, out_path, schema, part_dir):
    # Merge sorted runs into a single output Parquet file
    print('Start merging')
    brw = BatchedRunWriter(out_path, schema, batch_size)
    key_f = lambda row: reverse_path(row['max_stars_repo_path'])

    # Initialize generators for each run
    gens = [gen_parquet_lines(f'{part_dir}/run-{i}.parquet') for i in range(nruns)]
    heaparr = []
    for i, gen in enumerate(gens):
        row = next(gen)
        key = key_f(row)
        heaparr.append((key, i, row))
    heapq.heapify(heaparr)
    
    while heaparr:
        # Extract the smallest element from the heap
        _, i, line = heapq.heappop(heaparr)
        brw.write(line)
        # Read the next line from the same run
        line = next(gens[i])
        if line is not None:
            heapq.heappush(heaparr, (key_f(line), i, line))
    brw.close()
    print('closed:', out_path)

def main():
    if len(sys.argv) != 3:
        print('Usage: python3', sys.argv[0], '<data_path> <out_path>', file=sys.stderr)
        exit(-1)

    # Get input and output paths from command line arguments
    data_path = sys.argv[1]
    out_path = sys.argv[2]

    # Parameters
    M = 1024 * 1024  # 100MB
    batch_size = 10 ** 4

    # Extract directory from out_path and create part_dir if it does not exist
    out_dir = '/'.join(out_path.split('/')[:-1])
    part_dir = out_dir + '/parts'
    os.makedirs(part_dir, exist_ok=True)

    # Open the input Parquet file
    input_file = pq.ParquetFile(data_path)
    print('input rows:', input_file.metadata.num_rows)

    # Convert the ParquetSchema to a Schema object acceptable by ParquetWriter
    schema = input_file.schema.to_arrow_schema()

    # Iterate over batches and write them to the output file
    nruns = 0
    part_path = f'{part_dir}/run-{nruns}.parquet'
    writer = pq.ParquetWriter(part_path, schema, compression=None)
    acc = 0
    debug = 0
    for batch in input_file.iter_batches(batch_size=batch_size):
        if acc > M:
            print(part_path)
            writer.close()
            nruns += 1
            part_path = f'{part_dir}/run-{nruns}.parquet'
            writer = pq.ParquetWriter(part_path, schema, compression=None)
            acc = 0
            print(debug)
            debug = 0
        table = pa.Table.from_batches([batch])
        debug += table.shape[0]
        writer.write_table(table)
        acc += batch.nbytes
    print(debug)
    writer.close()
    nruns += 1

    print('nruns:', nruns)

    # Read again the Parquet files (runs) and sort them in main memory
    debug = 0
    for pid in range(nruns):
        parquet_file = pq.ParquetFile(f'{part_dir}/run-{pid}.parquet')
        batch = parquet_file.read()
        df = batch.to_pandas()

        # Sort the DataFrame in place using the reverse_path function applied on column max_stars_repo_path
        df['tmp'] = df['max_stars_repo_path'].apply(reverse_path)
        df.sort_values(by='tmp', inplace=True)
        df.drop(columns=['tmp'], inplace=True)

        writer = pq.ParquetWriter(f'{part_dir}/run-{pid}.parquet', schema, compression=None)
        writer.write_table(pa.Table.from_pandas(df, schema=schema))
        writer.close()
        print(f'sorted run-{pid}.parquet : {df.shape[0]}')
        debug += df.shape[0]
    print('Total:', debug)

    # Merge sorted runs into the final output file
    merge_sort(nruns, batch_size, out_path, schema, part_dir)

if __name__ == '__main__':
    main()
    print('Done')
