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
will have a format: ```row_key + _separator_ + col_key```. For now, the separator is ';',
but this may be a subject to change.

To handle timestamps, and using it to sort keys, TimestampComparator class will be used.
It implements the interface of the Comparator abstract class, and it basically handles the logic of
sorting values by timestamps.

Transactions will be done using ```rocksdb::WriteBatch``` objects. They allow to perform
batch operations and perform the atomically across column families, and they let every modification
inside them to have a different timestamp of the modification.