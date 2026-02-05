# Persistence Design

This file contains a basic design of adding persistence to the emulator.

Persistence itself will be done using rocksdb.

The general idea is to reuse as much of the existing code as possible. This means that for classes such as
Table, ColumnFamily, RowTransaction etc. there should be a similar class wrapping rocksdb functionality.
This way, functions responsible for handling gRPC requests on the server side should not change much.

As there is no concept of a table in rocksdb, each table in the emulator
will be represented by a separate database instance. This way, it will be easier to
use multiple tables, and each one of them will have a separate directory in the file system.

Because rocksdb supports a concept of a column family, each column family in the
emulator will have a backing column family in the rocksdb (represented by 
a ColumnFamilyHandle object, wrapped inside a PersistentColumnFamily class).

There is no concept of a row inside rocksdb. Because of that, each key in the database
will have a format: ```row_key#col_key#(~timestamp)```. '~' performed on the timestamp will preserve
the required ordering of keys. # is a separator in the form "\x00\x01". The "logical" separator is ';', and 0x01
is there to distinguish it from the ';' already present in row_key or col_key. ';' present in them are swapped for
";\xFF". Form implementation details, see "key_coder.h".

Transactions will be done using pessimistic transactions built in the RocksDB. To use them,
we use ```rocksdb::TransactionDB``` instead of the ```rocksdb::DB``` as the storage for a table.