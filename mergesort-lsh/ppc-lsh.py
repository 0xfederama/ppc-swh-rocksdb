import pandas as pd
import pyarrow.parquet as pq
import numpy as np
import subprocess, os

import tlsh
import chk

#from sklearn.cluster import KMeans
#from scipy.cluster.vq import whiten

#from sklearn_extra.cluster import KMedoids
#from sklearn.neighbors import NearestNeighbors

import time

"""
Legge un parquet ordinato secondo qualche metrica e ne calcola le distanze tra i contenuti nel dominio degli LSH
Ci sono altre funzioni definite, ma alm momento non vengono usate/inveocate
AL termine, viene disegnata su file PNG la matrice delle distanze tutti contro tutti
"""

size = '4M'
parq_path = f'/disk2/federico/the-stack/small/the-stack-{size}.parquet'

output_base_path = '/disk2/tosoni/test'
sorted_path = f'{output_base_path}/sorted-{size}.parquet' #f'/disk2/tosoni/the-stack-{size}.parquet.sorted' #f'{parq_path}.sorted'
compressed_path = f'/disk2/tosoni/the-stack-{size}.parquet.compressed'
delta_compressed_path = f'/disk2/tosoni/the-stack-{size}.parquet.delta_compressed' 

nsamples = 2
noversamples = 3 * nsamples
seed = 0x23

def time_diff_gen() :
    start_time = time.time()
    # codice da misurare
    while 1 :
        end_time = time.time()
        yield end_time - start_time
        start_time = end_time

def create_fingerprints(content: str) -> chk.CompoundHashKey :
    b = content.encode('utf-8')
    tlsh_hash = tlsh.hash(b)
    return chk.CompoundHashKey(tlsh_hash[8:])

# Function to compress content using zstd
def compress_content(content, mid=None, tmp='tmp.txt', tmp_zstd='tmp.zst'):
    openmode = 'wb' if isinstance(content, (bytes, bytearray)) else 'w'
    # Write content to a temporary file
    with open(tmp, openmode) as temp_file:
        temp_file.write(content)
    
    # Compress the file using zstd
    if mid is None:
        subprocess.run(['zstd', tmp, '-o', tmp_zstd])
    else:
        assert(mid >= 0)
        subprocess.run(['zstd', '-D', f'medoids/{mid}.txt', tmp, '-o', tmp_zstd])
    
    # Read the compressed content back
    with open(tmp_zstd, 'rb') as compressed_file:
        compressed_content = compressed_file.read()
    
    # Clean up temporary files
    os.remove(tmp)
    os.remove(tmp_zstd)
    
    return compressed_content

def reverse_string(input_str):
    last_dot_index = input_str.rfind('.')
    ending = input_str[last_dot_index+1:]
    input_str = input_str[:last_dot_index]
    parts = input_str.split('/')
    reversed_parts = parts[::-1]
    output_str = ending + '.' + '/'.join(reversed_parts)
    return output_str

time_diff = time_diff_gen()


#Read again, content only
sorted_file = pq.ParquetFile(sorted_path)
dataframes = []
for batch in sorted_file.iter_batches(
    columns=[
        #"hexsha",
        #"max_stars_repo_path",
        #"max_stars_repo_name",
        "content",
        #"size",
    ]
) :
    batch_df = batch.to_pandas()
    batch_df['fingerprints'] = batch_df['content'].apply(
        create_fingerprints
    )
    dataframes.append(batch_df)
batch_df = None #safety
metainfo_df = pd.concat(dataframes, ignore_index=True)
print(time_diff.__next__())

#Sort the metainfo_df by fingerprints
metainfo_df = metainfo_df.sort_values(by='fingerprints')

#Compute statistics for the fingerprints
fingerprints = metainfo_df['fingerprints']
vcs = fingerprints.value_counts()

#print('stats...')
#occs = {}
#for chk,c in vcs.items() :
#    if c not in occs :
#        occs[c] = 0
#    occs[c] += 1
#print(occs)

from PIL import Image

def draw_matrix(matrix):
    # Dimensioni della matrice
    rows = len(matrix)
    cols = len(matrix[0])

    # Creazione di una nuova immagine
    img = Image.new('L', (cols, rows))

    # Riempimento dell'immagine con i valori della matrice
    for i in range(rows):
        for j in range(cols):
            # Conversione del valore della matrice in un valore di grigio (0-255)
            gray_value = int((matrix[i][j] / chk.HashKey.m) * 255)
            img.putpixel((j, i), gray_value)

    # Salvataggio dell'immagine
    img.save(f'{output_base_path}/matrix-{size}.png')

# Esempio di matrice
print('Building matrix...')
matrix = [[chk_i.distance(chk_j) for chk_j in fingerprints] for chk_i in fingerprints]
print('Drawing matrix...')
draw_matrix(matrix)
