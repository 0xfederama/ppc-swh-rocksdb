from pyarrow.parquet import ParquetFile
import aimrocks
import time
import os

parquet_path = "/disk2/data/the-stack/the-stack-dedup_v1.parquet"

KiB = 1024
MiB = 1024 * 1024
GiB = 1024 * 1024 * 1024
DB_size = 0  # 10 * GiB  # make it 0 to create the whole contents_db

if __name__ == "__main__":
    print(f"Starting at {time.asctime()}, pid: {os.getpid()}")

    # check these options if creating the whole database
    compressions = [
        (aimrocks.CompressionType.no_compression, "uncomp"),
        # (aimrocks.CompressionType.zstd_compression, "zstd"),
        # (aimrocks.CompressionType.snappy_compression, "snappy"),  # default
        # (aimrocks.CompressionType.lz4_compression, "lz4"), # doesn't work
        # (aimrocks.CompressionType.zlib_compression, "zlib"),
    ]
    block_sizes = [
        (4 * KiB, "4K_block"),  # default
        # (256 * KiB, "256K_block"),
        # (4 * MiB, "4M_block"),
    ]

    for block in block_sizes:
        block_size = block[0]
        block_str = block[1]

        for compression in compressions:
            compr = compression[0]
            compr_str = compression[1]

            db_path = f"/disk2/federico/the-stack/contents_db-{compr_str}-{block_str}"
            opts = aimrocks.Options()
            opts.compression = compr
            opts.create_if_missing = True
            opts.error_if_exists = True
            opts.table_factory = aimrocks.BlockBasedTableFactory(block_size=block_size)
            db = aimrocks.DB(db_path, opts, read_only=False)

            pf = ParquetFile(parquet_path)
            batch_write = aimrocks.WriteBatch()
            size = 0
            print(f"Creating db {db_path}: {time.asctime()}")
            start = time.time()
            tmp_size = 0
            prints = 1
            for batch in pf.iter_batches(columns=["hexsha", "content"]):
                for i, sha in enumerate(batch["hexsha"]):
                    sha = str(sha)
                    content = batch["content"][i]
                    content = str(content)
                    batch_write.put(str.encode(sha), str.encode(content))
                    size += len(content)
                    tmp_size += len(content)
                    if tmp_size > 50 * GiB:
                        print(f"{50 * prints} GB: {time.asctime()}")
                        tmp_size = 0
                    if size > DB_size and DB_size != 0:
                        break
                db.write(batch_write)
                batch_write.clear()
                if size > DB_size & DB_size != 0:
                    break

            # write the remainings of the write batch
            db.write(batch_write)
            end = time.time()
            print(f"Created in {round(end-start)} sec")

    print(f"Ending at {time.asctime()}")
