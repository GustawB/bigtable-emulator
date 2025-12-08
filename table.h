// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_TABLE_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_TABLE_H

#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "column_family.h"
#include "filter.h"
#include "range_set.h"
#include "row_streamer.h"
#include "table.h"
#include <google/bigtable/admin/v2/bigtable_table_admin.pb.h>
#include <google/bigtable/admin/v2/table.pb.h>
#include <google/bigtable/v2/bigtable.pb.h>
#include <google/bigtable/v2/data.pb.h>
#include <google/protobuf/field_mask.pb.h>
#include <rocksdb/db.h>
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <stack>
#include <string>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

    class RowTransaction;

    struct RestoreValue {
        ColumnFamily& column_family;
        std::string column_qualifier;
        std::chrono::milliseconds timestamp;
        std::string value;
    };

    struct DeleteValue {
        ColumnFamily& column_family;
        std::string column_qualifier;
        std::chrono::milliseconds timestamp;
    };

    google::bigtable::v2::ReadModifyWriteRowResponse
    FamiliesToReadModifyWriteResponse(
        std::string const& row_key,
        std::map<std::string, ColumnFamily> const& families);

/// Objects of this class represent Bigtable tables.
class Table : public std::enable_shared_from_this<Table> {
 public:
  static StatusOr<std::shared_ptr<Table>> Create(
      std::string const& table_name, google::bigtable::admin::v2::Table schema,
      bool should_persist, std::string const& data_root = "/root/");

  virtual std::shared_ptr<Table> get() = 0;

  virtual google::bigtable::admin::v2::Table GetSchema() const = 0;

  virtual Status Update(google::bigtable::admin::v2::Table const& new_schema,
                        google::protobuf::FieldMask const& to_update) = 0;

  virtual StatusOr<google::bigtable::admin::v2::Table> ModifyColumnFamilies(
      google::bigtable::admin::v2::ModifyColumnFamiliesRequest const&
          request) = 0;

  virtual bool IsDeleteProtected() const = 0;

  virtual StatusOr<google::bigtable::v2::CheckAndMutateRowResponse>
  CheckAndMutateRow(
      google::bigtable::v2::CheckAndMutateRowRequest const& request) = 0;

  virtual Status MutateRow(
      google::bigtable::v2::MutateRowRequest const& request) = 0;

  /**
   * TODO: Think about this; this is called in the server.cc in some kind of
   * loop. For the RocksDB, maybe we could do it as one operation, so it would
   * be nice to instead extract the logic from server to the table. Right now
   * however, I want everything to compile.
   */
  virtual Status DoMutationsWithPossibleRollbackLocked(
      std::string const& row_key,
      google::protobuf::RepeatedPtrField<google::bigtable::v2::Mutation> const&
          mutations) = 0;

  virtual StatusOr<CellStream> CreateCellStream(
      std::shared_ptr<StringRangeSet> range_set,
      absl::optional<google::bigtable::v2::RowFilter>) const = 0;

  virtual Status ReadRows(google::bigtable::v2::ReadRowsRequest const& request,
                          RowStreamer& row_streamer) const = 0;

  virtual StatusOr<::google::bigtable::v2::ReadModifyWriteRowResponse>
  ReadModifyWriteRow(
      google::bigtable::v2::ReadModifyWriteRowRequest const& request) = 0;

  Status SampleRowKeys(
      double pass_probability,
      grpc::ServerWriter<google::bigtable::v2::SampleRowKeysResponse>* writer);

  virtual Status DropRowRange(
      ::google::bigtable::admin::v2::DropRowRangeRequest const& request) = 0;

  virtual ~Table() = default;

 protected:
    friend class RowTransaction;
    
  Status PrepareSchema();

  template <typename MESSAGE>
  StatusOr<std::reference_wrapper<ColumnFamily>> FindColumnFamily(
      MESSAGE const& message) const;

    virtual std::unique_ptr<RowTransaction> NewRowTransaction(std::shared_ptr<Table> table, std::string const& row_key) const = 0;

  mutable std::mutex mu_;
  google::bigtable::admin::v2::Table schema_;
  std::map<std::string, std::shared_ptr<ColumnFamily>> column_families_;
};

    class RowTransaction {
 public:
  explicit RowTransaction(std::shared_ptr<Table> table,
                          std::string const& row_key)
      : table_(std::move(table)), row_key_(row_key) {}
  virtual ~RowTransaction() = default;

  virtual Status commit() = 0;

  // timestamp_override, if provided, will be used instead of
  // set_cell.timestamp. The override is used to set the timestamp to
  // the server time in case a timestamp <= 0 is provided.
  virtual Status SetCell(
      ::google::bigtable::v2::Mutation_SetCell const& set_cell,
      absl::optional<std::chrono::milliseconds> timestamp_override) = 0;
  Status AddToCell(
      ::google::bigtable::v2::Mutation_AddToCell const& add_to_cell,
      absl::optional<std::chrono::milliseconds> timestamp_override);
  virtual Status MergeToCell(
      ::google::bigtable::v2::Mutation_MergeToCell const& merge_to_cell) = 0;
  virtual Status DeleteFromColumn(
      ::google::bigtable::v2::Mutation_DeleteFromColumn const&
          delete_from_column) = 0;
  virtual Status DeleteFromFamily(
      ::google::bigtable::v2::Mutation_DeleteFromFamily const&
          delete_from_family) = 0;
  virtual Status DeleteFromRow() = 0;

  virtual StatusOr<::google::bigtable::v2::ReadModifyWriteRowResponse>
  ReadModifyWriteRow(
      google::bigtable::v2::ReadModifyWriteRowRequest const& request) = 0;

 protected:
  virtual Status PerformAddToCell(
      ::google::bigtable::v2::Mutation_AddToCell const& add_to_cell,
      ColumnFamily cf, std::chrono::milliseconds ts_ms, std::string& value) = 0;

  std::shared_ptr<Table> table_;
  // row_key_ is initialized from the request proto and therefore it
  // is safe to access it while the mutation request is ongoing. We
  // store a reference to it to avoid copying a potentially very large
  // (up to 4KB) value.
  std::string const& row_key_;
};

class InMemoryRowTransaction : public RowTransaction {
 public:
  explicit InMemoryRowTransaction(std::shared_ptr<Table> table,
                                  std::string const& row_key)
      : RowTransaction(std::move(table), row_key) {}

  ~InMemoryRowTransaction() override {
    if (!committed_) {
      Undo();
    }
  };

  Status commit() override {
    committed_ = true;
    return Status();
  }

  // timestamp_override, if provided, will be used instead of
  // set_cell.timestamp. The override is used to set the timestamp to
  // the server time in case a timestamp <= 0 is provided.
  Status SetCell(::google::bigtable::v2::Mutation_SetCell const& set_cell,
                 absl::optional<std::chrono::milliseconds> timestamp_override =
                     absl::nullopt) override;
  Status MergeToCell(::google::bigtable::v2::Mutation_MergeToCell const&
                         merge_to_cell) override;
  Status DeleteFromColumn(
      ::google::bigtable::v2::Mutation_DeleteFromColumn const&
          delete_from_column) override;
  Status DeleteFromFamily(
      ::google::bigtable::v2::Mutation_DeleteFromFamily const&
          delete_from_family) override;
  Status DeleteFromRow() override;

  StatusOr<::google::bigtable::v2::ReadModifyWriteRowResponse>
  ReadModifyWriteRow(
      google::bigtable::v2::ReadModifyWriteRowRequest const& request) override;

 protected:
  Status PerformAddToCell(
      ::google::bigtable::v2::Mutation_AddToCell const& add_to_cell,
      ColumnFamily cf, std::chrono::milliseconds ts_ms,
      std::string& value) override;

 private:
  void Undo();

  bool committed_ = false;
  std::stack<absl::variant<DeleteValue, RestoreValue>> undo_;
};

class PersistentRowTransaction : public RowTransaction {
 public:
  explicit PersistentRowTransaction(std::shared_ptr<Table> table,
                                    std::string const& row_key)
      : RowTransaction(std::move(table), row_key), txn_(0, 0, 0, 16){};

  Status commit() override;
  Status SetCell(::google::bigtable::v2::Mutation_SetCell const& set_cell,
                 absl::optional<std::chrono::milliseconds> timestamp_override =
                     absl::nullopt) override;
  Status MergeToCell(::google::bigtable::v2::Mutation_MergeToCell const&
                         merge_to_cell) override;
  Status DeleteFromColumn(
      ::google::bigtable::v2::Mutation_DeleteFromColumn const&
          delete_from_column) override;
  Status DeleteFromFamily(
      ::google::bigtable::v2::Mutation_DeleteFromFamily const&
          delete_from_family) override;
  Status DeleteFromRow();

  StatusOr<::google::bigtable::v2::ReadModifyWriteRowResponse>
  ReadModifyWriteRow(
      google::bigtable::v2::ReadModifyWriteRowRequest const& request) override;

 protected:
  Status PerformAddToCell(
      ::google::bigtable::v2::Mutation_AddToCell const& add_to_cell,
      ColumnFamily cf, std::chrono::milliseconds ts_ms,
      std::string& value) override;

 private:
  rocksdb::WriteBatch txn_;
};

class InMemoryTable : public Table {
 public:
  static StatusOr<std::shared_ptr<Table>> Create(
      google::bigtable::admin::v2::Table schema);

  google::bigtable::admin::v2::Table GetSchema() const override;

  Status Update(google::bigtable::admin::v2::Table const& new_schema,
                google::protobuf::FieldMask const& to_update) override;

  StatusOr<google::bigtable::admin::v2::Table> ModifyColumnFamilies(
      google::bigtable::admin::v2::ModifyColumnFamiliesRequest const& request)
      override;

  bool IsDeleteProtected() const override;

  StatusOr<google::bigtable::v2::CheckAndMutateRowResponse> CheckAndMutateRow(
      google::bigtable::v2::CheckAndMutateRowRequest const& request) override;

  Status MutateRow(
      google::bigtable::v2::MutateRowRequest const& request) override;

  Status DoMutationsWithPossibleRollbackLocked(
      std::string const& row_key,
      google::protobuf::RepeatedPtrField<google::bigtable::v2::Mutation> const&
          mutations) override {
    std::lock_guard<std::mutex> lock(mu_);

    return DoMutationsWithPossibleRollback(row_key, mutations);
  }

  StatusOr<CellStream> CreateCellStream(
      std::shared_ptr<StringRangeSet> range_set,
      absl::optional<google::bigtable::v2::RowFilter>) const override;

  Status ReadRows(google::bigtable::v2::ReadRowsRequest const& request,
                  RowStreamer& row_streamer) const override;

  StatusOr<::google::bigtable::v2::ReadModifyWriteRowResponse>
  ReadModifyWriteRow(
      google::bigtable::v2::ReadModifyWriteRowRequest const& request) override;

  std::map<std::string, std::shared_ptr<ColumnFamily>>::iterator begin() {
    return column_families_.begin();
  }
  std::map<std::string, std::shared_ptr<ColumnFamily>>::iterator end() {
    return column_families_.end();
  }
  std::map<std::string, std::shared_ptr<ColumnFamily>>::iterator find(
      std::string const& column_family) {
    return column_families_.find(column_family);
  }

  std::shared_ptr<Table> get() override { return shared_from_this(); }

  Status DropRowRange(::google::bigtable::admin::v2::DropRowRangeRequest const&
                          request) override;

  ~InMemoryTable() override = default;

 protected:
  Status Construct(google::bigtable::admin::v2::Table schema);

    std::unique_ptr<RowTransaction> NewRowTransaction(std::shared_ptr<Table> table, std::string const& row_key) const override {
        return std::make_unique<InMemoryRowTransaction>(std::move(table), row_key);
    }

 private:
  InMemoryTable() = default;
  friend class RowSetIterator;

  bool IsDeleteProtectedNoLock() const;
  Status DoMutationsWithPossibleRollback(
      std::string const& row_key,
      google::protobuf::RepeatedPtrField<google::bigtable::v2::Mutation> const&
          mutations);
};

class PersistentTable : public Table {
 public:
  static StatusOr<std::shared_ptr<Table>> Create(
      std::string const& data_root, std::string const& table_name,
      google::bigtable::admin::v2::Table schema);

  google::bigtable::admin::v2::Table GetSchema() const override;

  Status Update(google::bigtable::admin::v2::Table const& new_schema,
                google::protobuf::FieldMask const& to_update) override;

  StatusOr<google::bigtable::admin::v2::Table> ModifyColumnFamilies(
      google::bigtable::admin::v2::ModifyColumnFamiliesRequest const& request)
      override;

  bool IsDeleteProtected() const override;

  StatusOr<google::bigtable::v2::CheckAndMutateRowResponse> CheckAndMutateRow(
      google::bigtable::v2::CheckAndMutateRowRequest const& request) override;

  Status MutateRow(
      google::bigtable::v2::MutateRowRequest const& request) override;

  Status DoMutationsWithPossibleRollbackLocked(
      std::string const& row_key,
      google::protobuf::RepeatedPtrField<google::bigtable::v2::Mutation> const&
          mutations) override;

  StatusOr<CellStream> CreateCellStream(
      std::shared_ptr<StringRangeSet> range_set,
      absl::optional<google::bigtable::v2::RowFilter>) const override;

  Status ReadRows(google::bigtable::v2::ReadRowsRequest const& request,
                  RowStreamer& row_streamer) const override;

  StatusOr<::google::bigtable::v2::ReadModifyWriteRowResponse>
  ReadModifyWriteRow(
      google::bigtable::v2::ReadModifyWriteRowRequest const& request) override;

  Status DropRowRange(::google::bigtable::admin::v2::DropRowRangeRequest const&
                          request) override;

  ~PersistentTable() override = default;

  rocksdb::DB* ToRawPtr() { return db_.get(); }

  std::shared_ptr<Table> get() override { return shared_from_this(); }

protected:
    std::unique_ptr<RowTransaction> NewRowTransaction(std::shared_ptr<Table> table, std::string const& row_key) const override {
        return std::make_unique<PersistentRowTransaction>(std::move(table), row_key);
    }

 private:
  PersistentTable() = default;
  friend class PersistentRowTransaction;

  Status DoMutations(
      std::string const& row_key,
      google::protobuf::RepeatedPtrField<google::bigtable::v2::Mutation> const&
          mutations);

  /**
   * To close a rocksdb database, in the simplest case it is enough to delete a
   * pointer to it
   * (https://github.com/facebook/rocksdb/wiki/basic-operations#closing-a-database,
   * https://github.com/facebook/rocksdb/wiki/basic-operations#concurrency).
   * We also don't need to care about locking here, as rocksdb has internal
   * synchronization.
   */
  std::shared_ptr<rocksdb::DB> db_;
};

/**
 * A `AbstractCellStreamImpl` which streams filtered contents of the table.
 *
 * Underneath is essentially a collection of `FilteredColumnFamilyStream`s.
 * All filters applied to `FilteredColumnFamilyStream` are propagated to the
 * underlying `FilteredColumnFamilyStream`, except for `FamilyNameRegex`, which
 * is handled by this subclass.
 *
 * This class is public only to enable testing.
 */
class FilteredTableStream : public MergeCellStreams {
 public:
  explicit FilteredTableStream(
      std::vector<std::unique_ptr<FilteredColumnFamilyStream>> cf_streams)
      : MergeCellStreams(CreateCellStreams(std::move(cf_streams))) {}

  bool ApplyFilter(InternalFilter const& internal_filter) override;

 private:
  static std::vector<CellStream> CreateCellStreams(
      std::vector<std::unique_ptr<FilteredColumnFamilyStream>> cf_streams);
};

class FilteredPersistentTableStream : public MergeCellStreams {
 public:
  explicit FilteredPersistentTableStream(
      std::vector<std::unique_ptr<FilteredPersistentColumnFamilyStream>>
          cf_streams)
      : MergeCellStreams(CreateCellStreams(std::move(cf_streams))) {}

  bool ApplyFilter(InternalFilter const& internal_filter) override;

 private:
  static std::vector<CellStream> CreateCellStreams(
      std::vector<std::unique_ptr<FilteredPersistentColumnFamilyStream>>
          cf_streams);
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_TABLE_H
