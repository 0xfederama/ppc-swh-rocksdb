import aimrocks
import tlsh

KiB = 1024
MiB = 1024 * 1024 * 1024


class DB_PPC:
    def __init__(self, path: str):
        """
        Initializes the DB_PPC class.

        Args:
            db_path (str): Path where the RocksDB database is stored.
        """
        self.db_path = path
        self.db = None

    def create_db(
        self,
        block_size=16 * KiB,
        compr=aimrocks.CompressionType.zlib_compression,
        compr_level=6,
        order="ext-filename-nopath",
    ):
        """
        Create a new database with the given options.

        Args:
            block_size (int, optional): The size of the blocks in the database. Defaults to 16 KiB.
            compr (aimrocks.CompressionType, optional): The compression type to use. Defaults to aimrocks.CompressionType.zlib_compression.
            compr_level (int, optional): The compression level to use. Defaults to 6.
            order (str): The order of the elements to use. Defaults to ext-filename-nopath.
        """
        if order not in ["rev-filename", "ext-filename-nopath", "tlsh"]:
            raise Exception(
                'Order must be either "rev-filename", "ext-filename-nopath", "tlsh"'
            )
        opts = aimrocks.Options()
        opts.create_if_missing = True
        opts.error_if_exists = True
        opts.allow_mmap_reads = True
        opts.paranoid_checks = False
        opts.use_adaptive_mutex = True
        # compression and block
        opts.compression = compr
        if compr_level != 0:
            opts.compression_opts = {"level": compr_level}
        opts.table_factory = aimrocks.BlockBasedTableFactory(block_size=block_size)
        self.db = aimrocks.DB(self.db_path, opts, read_only=False)
        self.order = order

    def open_db(
        self,
        block_size=16 * KiB,
        compr=aimrocks.CompressionType.zlib_compression,
        compr_level=6,
        order="ext-filename-nopath",
    ):
        """
        Open an existing database with the given options.

        Args:
            block_size (int, optional): The size of the blocks in the database. Defaults to 16 KiB.
            compr (aimrocks.CompressionType, optional): The compression type to use. Defaults to aimrocks.CompressionType.zlib_compression.
            compr_level (int, optional): The compression level to use. Defaults to 6.
            order (str): The order of the elements to use. Defaults to ext-filename-nopath.
        """
        if order not in ["rev-filename", "ext-filename-nopath", "tlsh"]:
            raise Exception(
                'Order must be either "rev-filename", "ext-filename-nopath", "tlsh"'
            )
        opts = aimrocks.Options()
        opts.create_if_missing = False
        opts.error_if_exists = False
        opts.allow_mmap_reads = True
        opts.paranoid_checks = False
        opts.use_adaptive_mutex = True
        # compression and block
        opts.compression = compr
        if compr_level != 0:
            opts.compression_opts = {"level": compr_level}
        opts.table_factory = aimrocks.BlockBasedTableFactory(block_size=block_size)
        self.db = aimrocks.DB(self.db_path, opts, read_only=False)
        self.order = order

    def insert_single(self, key: bytes, value: bytes):
        """
        Inserts a single key-value pair into the database.

        Args:
            key (bytes): The key to insert.
            value (bytes): The value to insert.

        Raises:
            Exception: If the database is not open.
        """
        if not self.db:
            raise Exception("Database is not open")
        self.db.put(key, value)

    def insert_batch(self, keyvalue_pairs: dict[bytes, bytes]):
        """
        Inserts multiple key-value pairs into the database in a single batch operation.

        Args:
            keyvalue_pairs (dict[bytes, bytes]): A dictionary containing the key-value pairs to be inserted.

        Raises:
            Exception: If the database is not open.
        """
        if not self.db:
            raise Exception("Database is not open")
        batch_write = aimrocks.WriteBatch()
        for key, value in keyvalue_pairs.items():
            batch_write.put(key, value)
        self.db.write(batch_write)

    def single_get(self, key: bytes) -> bytes:
        """
        Retrieve a value from the database for a given key.

        Args:
            key (bytes): The key to look up in the database.

        Returns:
            bytes: The value associated with the key, decoded as a UTF-8 string.
                   Returns None if the key does not exist in the database.

        Raises:
            Exception: If the database is not open.
        """
        if not self.db:
            raise Exception("Database is not open")
        value = self.db.get(key)
        return value.decode("utf-8") if value else None

    def multi_get(self, keys: list[bytes]) -> list[bytes]:
        """
        Retrieve multiple values from the database for the given list of keys.

        Args:
            keys (list[bytes]): A list of keys to retrieve values for.

        Returns:
            list[bytes]: A list of values corresponding to the provided keys.

        Raises:
            Exception: If the database is not open.
        """
        if not self.db:
            raise Exception("Database is not open")
        self.db.multi_get(keys)

    def delete_key(self, key: bytes):
        """
        Deletes a key-value pair from the database.

        Args:
            key (bytes): The key to be deleted from the database.

        Raises:
            Exception: If the database is not open.
        """
        if self.db is None:
            raise Exception("Database is not open")
        self.db.delete(key)

    def make_key(
        self, sha: str, filepath: str, size: int, max_size: int, content: str = None
    ) -> str:
        """
        Generates a key based on the specified ordering method.

        Args:
            sha (str): The SHA hash of the file.
            filepath (str): The path to the file.
            size (int): The size of the file.
            max_size (int): The maximum size for zero-padding the size.
            content (str, optional): The content of the file. Required if using "tlsh" ordering.

        Returns:
            str: The generated key.

        Raises:
            Exception: If content is not specified when using "tlsh" ordering.
        """
        size_len = len(str(max_size))
        size_str = str(size).zfill(size_len)
        match self.order:
            case "rev-filename":
                if filepath is None:
                    """"""
                key = f"{filepath[::-1]}_{size_str}-{sha}"
                return key
            case "ext-filename-nopath":
                if filepath is None:
                    """"""
                filename_with_extension = filepath.split("/")[-1]
                transformed_name = filename_with_extension
                if "." in transformed_name:
                    filename, extension = filename_with_extension.rsplit(".", 1)
                    transformed_name = f"{extension}.{filename}"
                key = f"{transformed_name}_{size_str}-{sha}"
                return key
            case "tlsh":
                if content is None:
                    raise Exception(
                        "You must specify the content when using TLSH ordering"
                    )
                if type(content) != str:
                    content = content.decode("latin-1")
                fingerprint = "0"
                if (
                    len(content) > 50 and len(content) < 1 * MiB
                ):  # requested by tlsh algorithm
                    # first 8 bytes are metadata
                    try:
                        fingerprint = tlsh.hash(str.encode(content))[8:]
                    except Exception as e:
                        print(f"Error while creating fingeprint, defaulting to 0.\n{e}")
                key = f"{fingerprint}_{size}-{sha}"
                return key
