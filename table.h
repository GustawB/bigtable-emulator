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
#include "absl/strings/match.h"
#include "column_family.h"
#include "filter.h"
#include "range_set.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "row_streamer.h"
#include <google/bigtable/admin/v2/bigtable_table_admin.pb.h>
#include <google/bigtable/admin/v2/table.pb.h>
#include <google/bigtable/v2/bigtable.pb.h>
#include <google/bigtable/v2/data.pb.h>
#include <google/protobuf/field_mask.pb.h>
#include <algorithm>
#include <chrono>
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

class Table;
class RowTransaction;
class InMemoryRowTransaction;
class PersistentRowTransaction;

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
    std::map<std::string, InMemoryColumnFamily> const& families);

class TableUtilities {
 public:
  virtual ~TableUtilities() = default;

  static StatusOr<std::shared_ptr<TableUtilities>> Create(
      google::bigtable::admin::v2::Table const& schema, bool should_persist,
      std::string const& data_root);

  virtual std::unique_ptr<RowTransaction> NewRowTransaction(
      std::shared_ptr<Table> table, std::string const& row_key) = 0;

  virtual StatusOr<CellStream> CreateCellStream(
      std::shared_ptr<StringRangeSet> range_set,
      absl::optional<google::bigtable::v2::RowFilter>) const = 0;

  virtual std::size_t GetRowCountEstimate() = 0;

  virtual void RemoveAllDataFromColumnFamilies() = 0;

  virtual void DropRowRange(std::string const& row_key_prefix) = 0;

  virtual Status Construct(
      google::bigtable::admin::v2::Table const& schema) = 0;

  virtual StatusOr<google::bigtable::admin::v2::Table> ModifyColumnFamilies(
      google::bigtable::admin::v2::ModifyColumnFamiliesRequest const& request,
      google::bigtable::admin::v2::Table schema) = 0;

  template <typename MESSAGE>
  StatusOr<std::shared_ptr<ColumnFamily>> FindColumnFamily(
      MESSAGE const& message) const;

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

 protected:
  std::map<std::string, std::shared_ptr<ColumnFamily>> column_families_;
};

class InMemoryTableUtilities
    : public TableUtilities,
      public std::enable_shared_from_this<InMemoryTableUtilities> {
 public:
  static StatusOr<std::shared_ptr<TableUtilities>> Create(
      google::bigtable::admin::v2::Table const& schema);

  std::unique_ptr<RowTransaction> NewRowTransaction(
      std::shared_ptr<Table> table, std::string const& row_key) override;

  StatusOr<CellStream> CreateCellStream(
      std::shared_ptr<StringRangeSet> range_set,
      absl::optional<google::bigtable::v2::RowFilter>) const override;

  std::size_t GetRowCountEstimate() override {
    std::size_t row_count_estimate = 0;
    for (auto const& cf : column_families_) {
      auto ccf = std::static_pointer_cast<InMemoryColumnFamily>(cf.second);
      row_count_estimate = std::max(ccf->size(), row_count_estimate);
    }

    return row_count_estimate;
  };

  void RemoveAllDataFromColumnFamilies() override {
    for (auto& column_family : column_families_) {
      column_family.second->RemoveAllDataFromColumnFamily();
    }
  }

  void DropRowRange(std::string const& row_key_prefix) override {
    for (auto& cf : column_families_) {
      auto ccf = std::static_pointer_cast<InMemoryColumnFamily>(cf.second);
      for (auto row_it = ccf->lower_bound(row_key_prefix);
           row_it != ccf->end();) {
        if (absl::StartsWith(row_it->first, row_key_prefix)) {
          row_it = ccf->erase(row_it);
        } else {
          break;
        }
      }
    }
  }

  Status Construct(google::bigtable::admin::v2::Table const& schema) override;

  StatusOr<google::bigtable::admin::v2::Table> ModifyColumnFamilies(
      google::bigtable::admin::v2::ModifyColumnFamiliesRequest const& request,
      google::bigtable::admin::v2::Table schema) override;

  std::shared_ptr<InMemoryTableUtilities> get() { return shared_from_this(); }
};

class PersistentTableUtilities
    : public TableUtilities,
      public std::enable_shared_from_this<PersistentTableUtilities> {
 public:
  static StatusOr<std::shared_ptr<TableUtilities>> Create(
      std::string const& data_root,
      google::bigtable::admin::v2::Table const& schema);

  std::unique_ptr<RowTransaction> NewRowTransaction(
      std::shared_ptr<Table> table, std::string const& row_key) override;

  StatusOr<CellStream> CreateCellStream(
      std::shared_ptr<StringRangeSet> range_set,
      absl::optional<google::bigtable::v2::RowFilter>) const override;

  std::size_t GetRowCountEstimate() override;

  void RemoveAllDataFromColumnFamilies() override;

  void DropRowRange(std::string const& row_key_prefix) override;

  Status Construct(google::bigtable::admin::v2::Table const& schema) override;

  StatusOr<google::bigtable::admin::v2::Table> ModifyColumnFamilies(
      google::bigtable::admin::v2::ModifyColumnFamiliesRequest const& request,
      google::bigtable::admin::v2::Table schema) override;

  std::shared_ptr<PersistentTableUtilities> get() { return shared_from_this(); }

 private:
  std::string table_name_;
  /**
   * To close a rocksdb database, in the simplest case it is enough to delete a
   * pointer to it
   * (https://github.com/facebook/rocksdb/wiki/basic-operations#closing-a-database,
   * https://github.com/facebook/rocksdb/wiki/basic-operations#concurrency).
   * We also don't need to care about locking here, as rocksdb has internal
   * synchronization.
   */
  std::shared_ptr<rocksdb::TransactionDB> db_;
};

/// Objects of this class represent Bigtable tables.
class Table : public std::enable_shared_from_this<Table> {
 public:
  static StatusOr<std::shared_ptr<Table>> Create(
      google::bigtable::admin::v2::Table schema, bool should_persist,
      std::string const& data_root = "/root/");

  std::shared_ptr<Table> get() { return shared_from_this(); }

  google::bigtable::admin::v2::Table GetSchema() const;

  Status Update(google::bigtable::admin::v2::Table const& new_schema,
                google::protobuf::FieldMask const& to_update);

  StatusOr<google::bigtable::admin::v2::Table> ModifyColumnFamilies(
      google::bigtable::admin::v2::ModifyColumnFamiliesRequest const& request);

  bool IsDeleteProtected() const;

  StatusOr<google::bigtable::v2::CheckAndMutateRowResponse> CheckAndMutateRow(
      google::bigtable::v2::CheckAndMutateRowRequest const& request);

  Status MutateRow(google::bigtable::v2::MutateRowRequest const& request);

  Status DoMutationsWithPossibleRollbackLocked(
      std::string const& row_key,
      google::protobuf::RepeatedPtrField<google::bigtable::v2::Mutation> const&
          mutations) {
    std::lock_guard<std::mutex> lock(mu_);

    return DoMutationsWithPossibleRollback(row_key, mutations);
  }

  Status ReadRows(google::bigtable::v2::ReadRowsRequest const& request,
                  RowStreamer& row_streamer) const;

  StatusOr<::google::bigtable::v2::ReadModifyWriteRowResponse>
  ReadModifyWriteRow(
      google::bigtable::v2::ReadModifyWriteRowRequest const& request);

  Status SampleRowKeys(
      double pass_probability,
      grpc::ServerWriter<google::bigtable::v2::SampleRowKeysResponse>* writer);

  Status DropRowRange(
      ::google::bigtable::admin::v2::DropRowRangeRequest const& request);

  // For testing only
  std::shared_ptr<TableUtilities> GetUtilities() { return utilities_; }

  ~Table() = default;

 private:
  friend class RowTransaction;
  friend class InMemoryRowTransaction;
  friend class PersistentRowTransaction;

  Status PrepareSchema();

  // template <typename MESSAGE>
  // StatusOr<std::shared_ptr<ColumnFamily>> FindColumnFamily(
  //     MESSAGE const& message) const;

  mutable std::mutex mu_;
  google::bigtable::admin::v2::Table schema_;

  Status Construct(google::bigtable::admin::v2::Table schema,
                   bool should_persist, std::string const& data_root);

  bool IsDeleteProtectedNoLock() const;

  Status DoMutationsWithPossibleRollback(
      std::string const& row_key,
      google::protobuf::RepeatedPtrField<google::bigtable::v2::Mutation> const&
          mutations);

  std::shared_ptr<TableUtilities> utilities_;
};

class RowTransaction {
 public:
  explicit RowTransaction(std::string const& row_key,
                          std::shared_ptr<TableUtilities> utilities)
      : utilities_(std::move(utilities)), row_key_(row_key) {}
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
  ;
  Status MergeToCell(
      ::google::bigtable::v2::Mutation_MergeToCell const& merge_to_cell);
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
  std::shared_ptr<TableUtilities> utilities_;

  virtual Status PerformAddToCell(
      ::google::bigtable::v2::Mutation_AddToCell const& add_to_cell,
      std::shared_ptr<ColumnFamily> cf, std::chrono::milliseconds ts_ms,
      std::string& value) = 0;

  // row_key_ is initialized from the request proto, and therefore it
  // is safe to access it while the mutation request is ongoing. We
  // store a reference to it to avoid copying a potentially very large
  // (up to 4KB) value.
  std::string const& row_key_;
};

class InMemoryRowTransaction : public RowTransaction {
 public:
  explicit InMemoryRowTransaction(std::shared_ptr<TableUtilities> utilities,
                                  std::string const& row_key)
      : RowTransaction(row_key, std::move(utilities)) {}

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
  Status SetCell(
      ::google::bigtable::v2::Mutation_SetCell const& set_cell,
      absl::optional<std::chrono::milliseconds> timestamp_override) override;
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
      std::shared_ptr<ColumnFamily> cf, std::chrono::milliseconds ts_ms,
      std::string& value) override;

 private:
  void Undo();

  bool committed_ = false;
  std::stack<absl::variant<DeleteValue, RestoreValue>> undo_;
};

class PersistentRowTransaction : public RowTransaction {
 public:
  explicit PersistentRowTransaction(std::shared_ptr<TableUtilities> utilities,
                                    std::string const& row_key,
                                    rocksdb::TransactionDB* db)
      : RowTransaction(row_key, std::move(utilities)) {
    txn_ = std::unique_ptr<rocksdb::Transaction>(
        db->BeginTransaction(rocksdb::WriteOptions()));
  }

  Status commit() override;
  Status SetCell(
      ::google::bigtable::v2::Mutation_SetCell const& set_cell,
      absl::optional<std::chrono::milliseconds> timestamp_override) override;
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
      std::shared_ptr<ColumnFamily> cf, std::chrono::milliseconds ts_ms,
      std::string& value) override;

 private:
  std::unique_ptr<rocksdb::Transaction> txn_;
};

class InMemoryTable : public Table {
 private:
  InMemoryTable() = default;
  friend class RowSetIterator;
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
class FilteredInMemoryTableStream : public MergeCellStreams {
 public:
  explicit FilteredInMemoryTableStream(
      std::vector<std::unique_ptr<AbstractCellStreamImpl>> cf_streams)
      : MergeCellStreams(CreateCellStreams(std::move(cf_streams))) {}

  bool ApplyFilter(InternalFilter const& internal_filter) override;

 private:
  static std::vector<CellStream> CreateCellStreams(
      std::vector<std::unique_ptr<AbstractCellStreamImpl>> cf_streams);
};

class FilteredPersistentTableStream : public MergeCellStreams {
 public:
  explicit FilteredPersistentTableStream(
      std::vector<std::unique_ptr<AbstractCellStreamImpl>> cf_streams)
      : MergeCellStreams(CreateCellStreams(std::move(cf_streams))) {}

  bool ApplyFilter(InternalFilter const& internal_filter) override;

 private:
  static std::vector<CellStream> CreateCellStreams(
      std::vector<std::unique_ptr<AbstractCellStreamImpl>> cf_streams);
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_TABLE_H
