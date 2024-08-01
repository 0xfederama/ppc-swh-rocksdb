import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import heapq


"""


"""

class BatchedRunWriter:

  schema = None
  pw = None
  buffer = dict()
  batch_size = 100000

  def __init__(self, outpath, schema, batch_size, compression=None): 
    self.schema = schema
    self.pw = pq.ParquetWriter(outpath, schema, compression=compression)
    self.buffer = {field:[] for field in schema}
    self.batch_size = batch_size

  def flush(self):
    if self.buffer :
      table = pa.Table.from_arrays([pa.array(col) for _,col in self.buffer.items()], schema=self.schema)
      self.pw.write_table(table)
      for col in self.buffer.values() :
        col.clear()
                        
  def write(self, row) :
    try :
      assert(len(row) == len(self.buffer))
    except AssertionError as e :
      print('row:', row)
      print('buffer:', self.buffer)
      raise e
    for val,(_,col) in zip(row,self.buffer.items()) :
      col.append(val)
    if len(col) == self.batch_size:
      self.flush()

  def close(self):
    self.flush()
    self.pw.close()

M = 1024 * 1024 # 100MB
batch_size = 10 ** 4

# Input Parquet file path
datasize = '4M' # 5rec, 1M, 8M, 64M, 256M, 1G, 4G, 10G, 200G, dedup_v1, 1G_minsize_4M, 2G_minsize_1M, 10G_minsize_1012K, 24G_minsize_990K
input_path = f'/disk2/federico/the-stack/small/the-stack-{datasize}.parquet'

# Output Parquet file path
output_base_path = '/disk2/tosoni/test'
output_path = 'data.parquet'

# Open the input Parquet file
input_file = pq.ParquetFile(input_path)
print('input rows:', input_file.metadata.num_rows)

# Convert the ParquetSchema to a Schema object acceptable by ParquetWriter
schema = input_file.schema.to_arrow_schema()

# Iterate over batches and write them to the output file
nruns = 0

output_path = f'{output_base_path}/run-{nruns}.parquet'
writer = pq.ParquetWriter(output_path, schema, compression=None)
acc = 0
debug = 0
for batch in input_file.iter_batches(batch_size=batch_size):
    #print(batch.shape())
    if acc > M:
        print(output_path)
        writer.close()
        nruns += 1
        output_path = f'{output_base_path}/run-{nruns}.parquet'
        writer = pq.ParquetWriter(output_path, schema, compression=None)
        acc = 0
        print(debug)
        debug = 0
    table = pa.Table.from_batches([batch])
    debug += table.shape[0]
    writer.write_table(table)
    acc += batch.nbytes
print(debug)
writer.close()
nruns+=1

print('nruns:', nruns)

#Read again the parquets (runs) and sort them in main memory

debug = 0
for pid in range(nruns):
    parquet_file = pq.ParquetFile(f'{output_base_path}/run-{pid}.parquet')
    batch = parquet_file.read()
    df = batch.to_pandas()
    df.sort_values(by='max_stars_repo_path', inplace=True)
    writer = pq.ParquetWriter(f'{output_base_path}/run-{pid}.parquet', schema, compression=None)
    writer.write_table(pa.Table.from_pandas(df, schema=schema))
    writer.close()
    print(f'sorted run-{pid}.parquet : {df.shape[0]}')
    debug += df.shape[0]
print('Total:', debug)

# Read the runs and sort them using M memory

def gen_parquet_lines(parquet_path):
    debug = 0
    parquet_file = pq.ParquetFile(parquet_path)
    for batch in parquet_file.iter_batches():
        batch_df = batch.to_pandas()
        for _,row in batch_df.iterrows():
            yield row
            debug += 1
    print(parquet_path, ':', debug)
    yield None

def merge_sort(nruns, batch_size, outpath=f'{output_base_path}/sorted-{datasize}.parquet'):
    print('Start merging')
    # Open the output Parquet file
    brw = BatchedRunWriter(outpath, schema, batch_size)
    sortcolumn = 'max_stars_repo_path'

    gens = [gen_parquet_lines(f'{output_base_path}/run-{i}.parquet') for i in range(nruns)]
    heaparr = []
    for i,gen in enumerate(gens) :
        row = next(gen)
        
        key = row[sortcolumn]
        heaparr.append((key,i,row))
    heapq.heapify(heaparr)
    
    while heaparr :
        #extract top queue element
        _,i,line = heapq.heappop(heaparr)

        #write to output
        brw.write(line)

        #read next line from the same run
        line = next(gens[i])
        if line is not None:
            heapq.heappush(heaparr, (line[sortcolumn],i,line))
    brw.close()
    print('closed:', outpath)

merge_sort(nruns, batch_size)

print('Done')
