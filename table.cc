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

#include "table.h"
#include "google/cloud/internal/big_endian.h"
#include "google/cloud/internal/make_status.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "absl/strings/str_format.h"
#include "absl/types/variant.h"
#include "column_family.h"
#include "filter.h"
#include "google/protobuf/util/field_mask_util.h"
#include "limits.h"
#include "range_set.h"
#include "re2/re2.h"
#include "row_streamer.h"
#include "utils.h"
#include <google/bigtable/admin/v2/bigtable_table_admin.pb.h>
#include <google/bigtable/admin/v2/table.pb.h>
#include <google/bigtable/admin/v2/types.pb.h>
#include <google/bigtable/v2/bigtable.pb.h>
#include <google/bigtable/v2/data.pb.h>
#include <google/protobuf/field_mask.pb.h>
#include <grpcpp/support/sync_stream.h>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <stack>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

namespace btadmin = ::google::bigtable::admin::v2;

StatusOr<std::shared_ptr<TableUtilities>> TableUtilities::Create(
    std::string const& table_name,
    google::bigtable::admin::v2::Table const& schema, bool should_persist,
    std::string const& data_root) {
  StatusOr<std::shared_ptr<TableUtilities>> maybe_utilities;
  if (!should_persist) {
    maybe_utilities = InMemoryTableUtilities::Create();
  } else {
    maybe_utilities =
        PersistentTableUtilities::Create(data_root, table_name, schema);
  }
  return maybe_utilities;
}

template <typename MESSAGE>
StatusOr<std::shared_ptr<ColumnFamily>> TableUtilities::FindColumnFamily(
    MESSAGE const& message) const {
  auto column_family_it = column_families_.find(message.family_name());
  if (column_family_it == column_families_.end()) {
    return NotFoundError(
        "No such column family.",
        GCP_ERROR_INFO().WithMetadata("mutation", message.DebugString()));
  }
  return column_family_it->second;
}
StatusOr<std::shared_ptr<TableUtilities>> InMemoryTableUtilities::Create() {
  return std::shared_ptr<TableUtilities>(new InMemoryTableUtilities);
}

std::unique_ptr<RowTransaction> InMemoryTableUtilities::NewRowTransaction(
    std::shared_ptr<Table> table, std::string const& row_key) {
  return std::make_unique<InMemoryRowTransaction>(this->get(), row_key);
}

StatusOr<CellStream> InMemoryTableUtilities::CreateCellStream(
    std::shared_ptr<StringRangeSet> range_set,
    absl::optional<google::bigtable::v2::RowFilter> maybe_row_filter) const {
  auto table_stream_ctor = [range_set = std::move(range_set), this] {
    std::vector<std::unique_ptr<AbstractCellStreamImpl>> per_cf_streams;
    per_cf_streams.reserve(column_families_.size());
    for (auto const& column_family : column_families_) {
      auto stream = column_family.second->GetFilteredColumnFamilyStream(
          range_set, column_family.first);
      per_cf_streams.emplace_back(std::move(stream));
    }
    return CellStream(std::make_unique<FilteredInMemoryTableStream>(
        std::move(per_cf_streams)));
  };

  if (maybe_row_filter.has_value()) {
    return CreateFilter(maybe_row_filter.value(), table_stream_ctor);
  }

  return table_stream_ctor();
}

Status InMemoryTableUtilities::Construct(
    google::bigtable::admin::v2::Table const& schema) {
  for (auto const& column_family_def : schema.column_families()) {
    absl::optional<google::bigtable::admin::v2::Type> opt_value_type =
        absl::nullopt;

    // Support for complex types (AddToCell aggregations, e.t.c.).
    if (column_family_def.second.has_value_type()) {
      opt_value_type = column_family_def.second.value_type();
    }

    if (opt_value_type.has_value()) {
      auto cf = InMemoryColumnFamily::ConstructAggregateColumnFamily(
          opt_value_type.value());
      if (!cf) {
        return cf.status();
      }
      auto gex = cf.value();
      column_families_.emplace(column_family_def.first, gex);
    } else {
      column_families_.emplace(column_family_def.first,
                               std::make_shared<InMemoryColumnFamily>());
    }
  }

  return Status();
}

StatusOr<google::bigtable::admin::v2::Table>
InMemoryTableUtilities::ModifyColumnFamilies(
    google::bigtable::admin::v2::ModifyColumnFamiliesRequest const& request,
    google::bigtable::admin::v2::Table schema) {
  auto new_column_families = column_families_;
  for (auto const& modification : request.modifications()) {
    if (modification.drop()) {
      if (schema.deletion_protection()) {
        return FailedPreconditionError(
            "The table has deletion protection.",
            GCP_ERROR_INFO().WithMetadata("modification",
                                          modification.DebugString()));
      }
      if (new_column_families.erase(modification.id()) == 0) {
        return NotFoundError("No such column family.",
                             GCP_ERROR_INFO().WithMetadata(
                                 "modification", modification.DebugString()));
      }
      if (schema.mutable_column_families()->erase(modification.id()) == 0) {
        return InternalError("Column family with no schema.",
                             GCP_ERROR_INFO().WithMetadata(
                                 "modification", modification.DebugString()));
      }
    } else if (modification.has_update()) {
      auto& cfs = *schema.mutable_column_families();
      auto cf_it = cfs.find(modification.id());
      if (cf_it == cfs.end()) {
        return NotFoundError("No such column family.",
                             GCP_ERROR_INFO().WithMetadata(
                                 "modification", modification.DebugString()));
      }

      using google::protobuf::util::FieldMaskUtil;

      using google::protobuf::util::FieldMaskUtil;
      google::protobuf::FieldMask effective_mask;
      if (modification.has_update_mask()) {
        effective_mask = modification.update_mask();
        if (!FieldMaskUtil::IsValidFieldMask<
                google::bigtable::admin::v2::ColumnFamily>(effective_mask)) {
          return InvalidArgumentError(
              "Update mask is invalid.",
              GCP_ERROR_INFO().WithMetadata("modification",
                                            modification.DebugString()));
        }
      } else {
        FieldMaskUtil::FromString("gc_rule", &effective_mask);
        if (!FieldMaskUtil::IsValidFieldMask<
                google::bigtable::admin::v2::ColumnFamily>(effective_mask)) {
          return InternalError("Default update mask is invalid.",
                               GCP_ERROR_INFO().WithMetadata(
                                   "mask", effective_mask.DebugString()));
        }
      }

      // Disallow the modification of the type of data stored in the
      // column family (the aggregate type -- which is currently the
      // only supported type -- can always be set during column family
      // creation).
      if (FieldMaskUtil::IsPathInFieldMask("value_type", effective_mask)) {
        return InvalidArgumentError(
            "The value_type cannot be changed after column family creation",
            GCP_ERROR_INFO().WithMetadata("mask",
                                          effective_mask.DebugString()));
      }

      FieldMaskUtil::MergeMessageTo(modification.update(), effective_mask,
                                    FieldMaskUtil::MergeOptions(),
                                    &(cf_it->second));
    } else if (modification.has_create()) {
      std::shared_ptr<ColumnFamily> cf;
      // Have we been asked to create an aggregate column family?
      if (modification.create().has_value_type()) {
        auto value_type = modification.create().value_type();
        auto maybe_cf =
            InMemoryColumnFamily::ConstructAggregateColumnFamily(value_type);
        if (!maybe_cf) {
          return maybe_cf.status();
        }
        cf = std::move(maybe_cf.value());
      } else {
        cf = std::make_shared<InMemoryColumnFamily>();
      }
      if (!new_column_families.emplace(modification.id(), cf).second) {
        return AlreadyExistsError(
            "Column family already exists.",
            GCP_ERROR_INFO().WithMetadata("modification",
                                          modification.DebugString()));
      }
      if (!schema.mutable_column_families()
               ->emplace(modification.id(), modification.create())
               .second) {
        return InternalError("Column family with schema but no data.",
                             GCP_ERROR_INFO().WithMetadata(
                                 "modification", modification.DebugString()));
      }
    } else {
      return UnimplementedError(
          "Unsupported modification.",
          GCP_ERROR_INFO().WithMetadata("modification",
                                        modification.DebugString()));
    }
  }
  // Defer destroying potentially large objects to after releasing the lock.
  column_families_.swap(new_column_families);
  return schema;
}

StatusOr<std::shared_ptr<TableUtilities>> PersistentTableUtilities::Create(
    std::string const& data_root, std::string const& table_name,
    google::bigtable::admin::v2::Table const& schema) {
  std::filesystem::path db_path = std::filesystem::path(data_root) / table_name;
  std::filesystem::path parent_path = db_path.parent_path();

  std::error_code ec;
  std::filesystem::create_directories(parent_path, ec);
  if (ec) {
    return InternalError(
        "failed to create directory: " + parent_path.string() +
            "; Error status: " + ec.message(),
        GCP_ERROR_INFO().WithMetadata("schema", schema.DebugString()));
  }

  rocksdb::Options options;
  rocksdb::TransactionDBOptions txn_options;
  txn_options.lock_mgr_handle.reset(rocksdb::NewRangeLockManager(nullptr));

  rocksdb::TransactionDB* raw_db = nullptr;

  options.create_if_missing = true;
  options.create_missing_column_families = false;

  rocksdb::Status status = rocksdb::TransactionDB::Open(
      options, txn_options, db_path.string(), &raw_db);

  if (!status.ok()) {
    return InternalError(
        "failed to create new rocksdb instance; " + status.ToString(),
        GCP_ERROR_INFO().WithMetadata("path", db_path.string()));
  }

  std::shared_ptr<PersistentTableUtilities> res(new PersistentTableUtilities);
  res->db_.reset(raw_db);  // Assuming db_ is a smart pointer.
  res->table_name_ = table_name;

  return StatusOr<std::shared_ptr<TableUtilities>>(std::move(res));
}

std::unique_ptr<RowTransaction> PersistentTableUtilities::NewRowTransaction(
    std::shared_ptr<Table> table, std::string const& row_key) {
  return std::make_unique<PersistentRowTransaction>(this->get(), row_key,
                                                    db_.get());
}

StatusOr<CellStream> PersistentTableUtilities::CreateCellStream(
    std::shared_ptr<StringRangeSet> range_set,
    absl::optional<google::bigtable::v2::RowFilter>) const {
  auto table_stream_ctor = [range_set = std::move(range_set), this] {
    std::vector<std::unique_ptr<AbstractCellStreamImpl>> per_cf_streams;
    per_cf_streams.reserve(column_families_.size());
    for (auto const& handle : column_families_) {
      auto stream =
          handle.second->GetFilteredColumnFamilyStream(range_set, handle.first);
      per_cf_streams.emplace_back(std::move(stream));
    }
    return CellStream(std::make_unique<FilteredPersistentTableStream>(
        std::move(per_cf_streams)));
  };

  return table_stream_ctor();
}

std::size_t PersistentTableUtilities::GetRowCountEstimate() {
  throw "UNIMPLEMENTED";
}

void PersistentTableUtilities::RemoveAllDataFromColumnFamilies() {
  throw "UNIMPLEMENTED";
}

void PersistentTableUtilities::DropRowRange(std::string const& row_key_prefix) {
  throw "UNIMPLEMENTED";
}

Status PersistentTableUtilities::Construct(
    google::bigtable::admin::v2::Table const& schema) {
  for (auto const& cfd : schema.column_families()) {
    absl::optional<google::bigtable::admin::v2::Type> opt_value_type =
        absl::nullopt;

    if (cfd.second.has_value_type()) {
      opt_value_type = cfd.second.value_type();
    }

    if (opt_value_type.has_value()) {
      auto new_cf = PersistentColumnFamily::ConstructAggregateColumnFamily(
          opt_value_type.value(), db_, table_name_);
      if (!new_cf) {
        return new_cf.status();
      }
      column_families_.emplace(cfd.first, new_cf.value());
    } else {
      // TODO: handle opts
      rocksdb::ColumnFamilyOptions opts;
      auto maybe_new_cf = PersistentColumnFamily::Create(db_, opts, cfd.first);
      if (!maybe_new_cf.ok()) {
        return InternalError(
            "failed to create column family " + cfd.first +
                "; Error status: " + maybe_new_cf.status().message(),
            GCP_ERROR_INFO().WithMetadata("schema", schema.DebugString()));
      }
      column_families_.emplace(cfd.first, maybe_new_cf.value());
    }
  }

  return Status();
}

StatusOr<google::bigtable::admin::v2::Table>
PersistentTableUtilities::ModifyColumnFamilies(
    google::bigtable::admin::v2::ModifyColumnFamiliesRequest const& request,
    google::bigtable::admin::v2::Table schema) {
  auto new_handles = column_families_;

  ModifyCfRollback rollback(db_);

  auto rollback_and_return =
      [&](Status const& original) -> StatusOr<btadmin::Table> {
    Status rb = rollback.Rollback();
    if (!rb.ok()) {
      return InternalError(
          "ModifyColumnFamilies failed and rollback failed: " + rb.message(),
          GCP_ERROR_INFO()
              .WithMetadata("original_error", original.message())
              .WithMetadata("request", request.DebugString()));
    }
    return original;
  };

  for (auto const& modification : request.modifications()) {
    if (modification.drop()) {
      if (schema.deletion_protection()) {
        return rollback_and_return(FailedPreconditionError(
            "The table has deletion protection.",
            GCP_ERROR_INFO().WithMetadata("modification",
                                          modification.DebugString())));
      }

      auto it = new_handles.find(modification.id());
      if (it == new_handles.end()) {
        return rollback_and_return(
            NotFoundError("No such column family.",
                          GCP_ERROR_INFO().WithMetadata(
                              "modification", modification.DebugString())));
      }

      auto cf_obj = it->second;

      auto persistent_cf =
          std::static_pointer_cast<PersistentColumnFamily>(cf_obj);

      std::shared_ptr<rocksdb::ColumnFamilyHandle> old_handle =
          persistent_cf->GetHandle();

      rocksdb::ColumnFamilyDescriptor desc;
      {
        rocksdb::Status s = old_handle->GetDescriptor(&desc);
        if (!s.ok()) {
          return rollback_and_return(InternalError(
              "Failed to get CF descriptor before drop: " + s.ToString(),
              GCP_ERROR_INFO().WithMetadata("modification",
                                            modification.DebugString())));
        }
      }

      rollback.RecordDropped(modification.id(), persistent_cf, old_handle,
                             desc);

      {
        rocksdb::Status s = db_->DropColumnFamily(old_handle.get());
        if (!s.ok()) {
          return rollback_and_return(InternalError(
              "Failed to drop column family in RocksDB: " + s.ToString(),
              GCP_ERROR_INFO().WithMetadata("modification",
                                            modification.DebugString())));
        }
      }

      new_handles.erase(modification.id());
      if (schema.mutable_column_families()->erase(modification.id()) == 0) {
        return rollback_and_return(
            InternalError("Column family with no schema.",
                          GCP_ERROR_INFO().WithMetadata(
                              "modification", modification.DebugString())));
      }

    } else if (modification.has_update()) {
      auto& cfs = *schema.mutable_column_families();
      auto cf_it = cfs.find(modification.id());
      if (cf_it == cfs.end()) {
        return rollback_and_return(
            NotFoundError("No such column family.",
                          GCP_ERROR_INFO().WithMetadata(
                              "modification", modification.DebugString())));
      }

      using google::protobuf::util::FieldMaskUtil;

      google::protobuf::FieldMask effective_mask;
      if (modification.has_update_mask()) {
        effective_mask = modification.update_mask();
        if (!FieldMaskUtil::IsValidFieldMask<
                google::bigtable::admin::v2::ColumnFamily>(effective_mask)) {
          return rollback_and_return(InvalidArgumentError(
              "Update mask is invalid.",
              GCP_ERROR_INFO().WithMetadata("modification",
                                            modification.DebugString())));
        }
      } else {
        FieldMaskUtil::FromString("gc_rule", &effective_mask);
        if (!FieldMaskUtil::IsValidFieldMask<
                google::bigtable::admin::v2::ColumnFamily>(effective_mask)) {
          return rollback_and_return(
              InternalError("Default update mask is invalid.",
                            GCP_ERROR_INFO().WithMetadata(
                                "mask", effective_mask.DebugString())));
        }
      }

      // Disallow the modification of the type of data stored in the
      // column family (the aggregate type -- which is currently the
      // only supported type -- can always be set during column family
      // creation).
      if (FieldMaskUtil::IsPathInFieldMask("value_type", effective_mask)) {
        return rollback_and_return(InvalidArgumentError(
            "The value_type cannot be changed after column family creation",
            GCP_ERROR_INFO().WithMetadata("mask",
                                          effective_mask.DebugString())));
      }

      FieldMaskUtil::MergeMessageTo(modification.update(), effective_mask,
                                    FieldMaskUtil::MergeOptions(),
                                    &(cf_it->second));

    } else if (modification.has_create()) {
      if (new_handles.find(modification.id()) != new_handles.end()) {
        return rollback_and_return(AlreadyExistsError(
            "Column family already exists.",
            GCP_ERROR_INFO().WithMetadata("modification",
                                          modification.DebugString())));
      }

      std::shared_ptr<PersistentColumnFamily> cf;
      // Have we been asked to create an aggregate column family?

      if (modification.create().has_value_type()) {
        auto value_type = modification.create().value_type();
        auto maybe_cf = PersistentColumnFamily::ConstructAggregateColumnFamily(
            value_type, db_, modification.id());
        if (!maybe_cf) {
          return rollback_and_return(maybe_cf.status());
        }
        cf = std::move(maybe_cf.value());
      } else {
        auto maybe_cf = PersistentColumnFamily::Create(
            db_, rocksdb::ColumnFamilyOptions(), modification.id());
        if (!maybe_cf.ok()) {
          return rollback_and_return(
              InternalError("Failed to create new column family; " +
                                maybe_cf.status().message(),
                            GCP_ERROR_INFO().WithMetadata(
                                "modification", modification.DebugString())));
        }
        cf = maybe_cf.value();
      }

      rollback.RecordCreated(modification.id(), cf->GetHandle());

      new_handles.emplace(modification.id(), cf);
      if (!schema.mutable_column_families()
               ->emplace(modification.id(), modification.create())
               .second) {
        return rollback_and_return(
            InternalError("Column family with schema but no data.",
                          GCP_ERROR_INFO().WithMetadata(
                              "modification", modification.DebugString())));
      }

    } else {
      return rollback_and_return(
          UnimplementedError("Unsupported modification.",
                             GCP_ERROR_INFO().WithMetadata(
                                 "modification", modification.DebugString())));
    }
  }

  rollback.Commit();

  // Defer destroying potentially large objects to after releasing the lock.
  column_families_.swap(new_handles);
  return schema;
}

StatusOr<std::shared_ptr<Table>> Table::Create(
    std::string const& table_name, google::bigtable::admin::v2::Table schema,
    bool should_persist, std::string const& data_root) {
  std::shared_ptr<Table> res(new Table);

  auto maybe_utilities = TableUtilities::Create(std::move(table_name), schema,
                                                should_persist, data_root);
  if (!maybe_utilities.ok()) {
    return maybe_utilities.status();
  }
  res->utilities_ = maybe_utilities.value();

  auto status = res->Construct(std::move(schema));
  if (!status.ok()) {
    return status;
  }

  return res;
}

google::bigtable::admin::v2::Table Table::GetSchema() const {
  std::lock_guard<std::mutex> lock(mu_);
  return schema_;
}

Status Table::PrepareSchema() {
  if (schema_.granularity() ==
      btadmin::Table::TIMESTAMP_GRANULARITY_UNSPECIFIED) {
    schema_.set_granularity(btadmin::Table::MILLIS);
  }
  if (schema_.cluster_states_size() > 0) {
    return InvalidArgumentError(
        "`cluster_states` not empty.",
        GCP_ERROR_INFO().WithMetadata("schema", schema_.DebugString()));
  }
  if (schema_.has_restore_info()) {
    return InvalidArgumentError(
        "`restore_info` not empty.",
        GCP_ERROR_INFO().WithMetadata("schema", schema_.DebugString()));
  }
  if (schema_.has_change_stream_config()) {
    return UnimplementedError(
        "`change_stream_config` not empty.",
        GCP_ERROR_INFO().WithMetadata("schema", schema_.DebugString()));
  }
  if (schema_.has_automated_backup_policy()) {
    return UnimplementedError(
        "`automated_backup_policy` not empty.",
        GCP_ERROR_INFO().WithMetadata("schema", schema_.DebugString()));
  }
  return Status();
}

// TODO: Persistency
Status Table::SampleRowKeys(
    double pass_probability,
    grpc::ServerWriter<google::bigtable::v2::SampleRowKeysResponse>* writer) {
  if (pass_probability <= 0.0) {
    return InvalidArgumentError(
        "The sampling probabality must be positive",
        GCP_ERROR_INFO().WithMetadata("provided sampling probability",
                                      absl::StrFormat("%f", pass_probability)));
  }

  auto sample_every =
      static_cast<std::uint64_t>(std::ceil(1.0 / pass_probability));

  std::lock_guard<std::mutex> lock(mu_);

  // First, stream all rows and cells and compute the offsets.
  auto all_rows_set = std::make_shared<StringRangeSet>(StringRangeSet::All());
  auto maybe_all_rows_stream =
      utilities_->CreateCellStream(all_rows_set, absl::nullopt);
  if (!maybe_all_rows_stream) {
    return maybe_all_rows_stream.status();
  }

  auto& stream = *maybe_all_rows_stream;

  absl::optional<std::string> first_row_key;
  // The first row read will be used as a constant estimate of row
  // sizes. If we are sampling 1/n rows, the value added to the offset
  // (which is to be regarded as the size of all the rows before the
  // sampled one) will be (n * row_size_estimate).
  //
  // That is every time a row is sampled, we do: offset += (n *
  // row_size_estimate).
  std::size_t row_size_estimate = 0;

  for (; stream; ++stream) {
    if (first_row_key.has_value() &&
        stream->row_key() != first_row_key.value()) {
      break;
    }

    first_row_key = stream->row_key();

    row_size_estimate += stream->row_key().size();
    row_size_estimate += stream->column_qualifier().size();
    row_size_estimate += stream->value().size();
    row_size_estimate += sizeof(stream->timestamp());
  }

  if (!first_row_key.has_value()) {
    // No rows in the table
    google::bigtable::v2::SampleRowKeysResponse resp;
    resp.set_row_key("");
    resp.set_offset_bytes(0);

    auto opts = grpc::WriteOptions();
    opts.set_last_message();

    writer->WriteLast(std::move(resp), opts);
    return Status();
  }

  std::int64_t offset_delta = sample_every * row_size_estimate;

  google::bigtable::v2::RowFilter sample_filter;
  sample_filter.set_row_sample_filter(pass_probability);

  auto maybe_stream = utilities_->CreateCellStream(all_rows_set, sample_filter);
  if (!maybe_stream) {
    return maybe_stream.status();
  }

  auto& sampled_stream = *maybe_stream;

  std::int64_t offset = 0;

  bool wrote_a_sample;

  for (; sampled_stream; sampled_stream.Next(NextMode::kRow)) {
    google::bigtable::v2::SampleRowKeysResponse resp;
    offset += offset_delta;
    resp.set_row_key(sampled_stream->row_key());
    resp.set_offset_bytes(offset);

    writer->Write(std::move(resp));

    wrote_a_sample = true;
  }

  // Cloud bigtable client tests expect that, if they populated the
  // table with at least one row, then at least one row sample is
  // returned.
  //
  // In such a case, return any string that represents the last key,
  // and an offset that is the estimated row size * the number of rows
  // in the largest column family. We can return any string because
  // the keys returned need not be in the table. See the proto
  // specification.
  if (!wrote_a_sample) {
    std::size_t row_count_estimate = utilities_->GetRowCountEstimate();

    std::int64_t this_offset = row_count_estimate * row_size_estimate;

    google::bigtable::v2::SampleRowKeysResponse resp;
    resp.set_row_key("last_key");
    resp.set_offset_bytes(this_offset);
    writer->Write(std::move(resp));

    offset += this_offset;
  }

  google::bigtable::v2::SampleRowKeysResponse resp;
  resp.set_row_key("");
  // Client test code expects offset_bytes to be strictly
  // increasing.
  resp.set_offset_bytes(offset + 1);
  auto opts = grpc::WriteOptions();
  opts.set_last_message();
  writer->WriteLast(std::move(resp), opts);

  return Status();
}

Status Table::Construct(google::bigtable::admin::v2::Table schema) {
  // Normally the constructor acts as a synchronization point. We don't have
  // that luxury here, so we need to make sure that the changes performed in
  // this member function are reflected in other threads. The simplest way to do
  // this is the mutex.
  std::lock_guard<std::mutex> lock(mu_);
  schema_ = std::move(schema);
  Status parse_result = PrepareSchema();
  if (!parse_result.ok()) return parse_result;
  utilities_->Construct(schema);
  return Status();
}

// NOLINTBEGIN(readability-function-cognitive-complexity)
StatusOr<btadmin::Table> Table::ModifyColumnFamilies(
    btadmin::ModifyColumnFamiliesRequest const& request) {
  std::cout << "Modify column families: " << request.DebugString() << std::endl;
  std::unique_lock<std::mutex> lock(mu_);

  auto maybe_new_schema = utilities_->ModifyColumnFamilies(request, schema_);
  if (!maybe_new_schema) {
    return maybe_new_schema.status();
  }
  auto new_schema = maybe_new_schema.value();

  schema_ = new_schema;
  lock.unlock();
  return new_schema;
}
// NOLINTEND(readability-function-cognitive-complexity)

Status Table::Update(google::bigtable::admin::v2::Table const& new_schema,
                     google::protobuf::FieldMask const& to_update) {
  std::cout << "Update schema: " << new_schema.DebugString()
            << " mask: " << to_update.DebugString() << std::endl;
  using google::protobuf::util::FieldMaskUtil;
  google::protobuf::FieldMask allowed_mask;
  FieldMaskUtil::FromString(
      "change_stream_config,"
      "change_stream_config.retention_period,"
      "deletion_protection",
      &allowed_mask);
  if (!FieldMaskUtil::IsValidFieldMask<google::bigtable::admin::v2::Table>(
          to_update)) {
    return InvalidArgumentError(
        "Update mask is invalid.",
        GCP_ERROR_INFO().WithMetadata("mask", to_update.DebugString()));
  }
  google::protobuf::FieldMask disallowed_mask;
  FieldMaskUtil::Subtract<google::bigtable::admin::v2::Table>(
      to_update, allowed_mask, &disallowed_mask);
  if (disallowed_mask.paths_size() > 0) {
    return UnimplementedError(
        "Update mask contains disallowed fields.",
        GCP_ERROR_INFO().WithMetadata("mask", disallowed_mask.DebugString()));
  }
  std::lock_guard<std::mutex> lock(mu_);
  FieldMaskUtil::MergeMessageTo(new_schema, to_update,
                                FieldMaskUtil::MergeOptions(), &schema_);
  return Status();
}

Status Table::MutateRow(google::bigtable::v2::MutateRowRequest const& request) {
  std::lock_guard<std::mutex> lock(mu_);

  return DoMutationsWithPossibleRollback(request.row_key(),
                                         request.mutations());
}

// NOLINTBEGIN(readability-function-cognitive-complexity)
Status Table::DoMutationsWithPossibleRollback(
    std::string const& row_key,
    google::protobuf::RepeatedPtrField<google::bigtable::v2::Mutation> const&
        mutations) {
  if (row_key.size() > kMaxRowLen) {
    return InvalidArgumentError(
        "The row_key is longer than 4KiB",
        GCP_ERROR_INFO().WithMetadata("row_key size",
                                      absl::StrFormat("%zu", row_key.size())));
  }

  std::unique_ptr<RowTransaction> row_transaction =
      utilities_->NewRowTransaction(this->get(), row_key);

  for (auto const& mutation : mutations) {
    if (mutation.has_set_cell()) {
      auto const& set_cell = mutation.set_cell();

      absl::optional<std::chrono::milliseconds> timestamp_override =
          absl::nullopt;

      if (set_cell.timestamp_micros() < -1) {
        return InvalidArgumentError(
            "Timestamp micros cannot be < -1.",
            GCP_ERROR_INFO().WithMetadata("mutation", mutation.DebugString()));
      }

      if (set_cell.timestamp_micros() == -1) {
        timestamp_override.emplace(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()));
      }

      auto status = row_transaction->SetCell(set_cell, timestamp_override);
      if (!status.ok()) {
        return status;
      }
    } else if (mutation.has_add_to_cell()) {
      auto const& add_to_cell = mutation.add_to_cell();

      absl::optional<std::chrono::milliseconds> timestamp_override =
          absl::nullopt;

      std::chrono::milliseconds timestamp = std::chrono::milliseconds::zero();

      if (add_to_cell.has_timestamp() &&
          add_to_cell.timestamp().has_raw_timestamp_micros()) {
        timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::microseconds(
                add_to_cell.timestamp().raw_timestamp_micros()));
      }

      // If no valid timestamp is provided, override with the system time.
      if (timestamp <= std::chrono::milliseconds::zero()) {
        timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch());
        timestamp_override.emplace(std::move(timestamp));
      }

      auto status = row_transaction->AddToCell(add_to_cell, timestamp_override);
      if (!status.ok()) {
        return status;
      }
    } else if (mutation.has_merge_to_cell()) {
      return UnimplementedError(
          "Unsupported mutation type.",
          GCP_ERROR_INFO().WithMetadata("mutation", mutation.DebugString()));
    } else if (mutation.has_delete_from_column()) {
      auto const& delete_from_column = mutation.delete_from_column();
      auto status = row_transaction->DeleteFromColumn(delete_from_column);
      if (!status.ok()) {
        return status;
      }
    } else if (mutation.has_delete_from_family()) {
      auto const& delete_from_family = mutation.delete_from_family();
      auto status = row_transaction->DeleteFromFamily(delete_from_family);
      if (!status.ok()) {
        return status;
      }
    } else if (mutation.has_delete_from_row()) {
      auto status = row_transaction->DeleteFromRow();
      if (!status.ok()) {
        return status;
      }
    } else {
      return UnimplementedError(
          "Unsupported mutation type.",
          GCP_ERROR_INFO().WithMetadata("mutation", mutation.DebugString()));
    }
  }

  // If we get here, all mutations on the row have succeeded. We can
  // commit and return which will prevent the destructor from undoing
  // the transaction.
  return row_transaction->commit();
}

bool FilteredInMemoryTableStream::ApplyFilter(
    InternalFilter const& internal_filter) {
  if (!absl::holds_alternative<FamilyNameRegex>(internal_filter) &&
      !absl::holds_alternative<ColumnRange>(internal_filter)) {
    return MergeCellStreams::ApplyFilter(internal_filter);
  }
  // internal_filter is either FamilyNameRegex or ColumnRange
  for (auto stream_it = unfinished_streams_.begin();
       stream_it != unfinished_streams_.end();) {
    auto* cf_stream =
        static_cast<FilteredInMemoryColumnFamilyStream*>(&(*stream_it)->impl());
    assert(cf_stream);

    if ((absl::holds_alternative<FamilyNameRegex>(internal_filter) &&
         !re2::RE2::PartialMatch(
             cf_stream->column_family_name(),
             *absl::get<FamilyNameRegex>(internal_filter).regex)) ||
        (absl::holds_alternative<ColumnRange>(internal_filter) &&
         absl::get<ColumnRange>(internal_filter).column_family !=
             cf_stream->column_family_name())) {
      stream_it = unfinished_streams_.erase(stream_it);
      continue;
    }

    if (absl::holds_alternative<ColumnRange>(internal_filter) &&
        absl::get<ColumnRange>(internal_filter).column_family ==
            cf_stream->column_family_name()) {
      cf_stream->ApplyFilter(internal_filter);
    }

    stream_it++;
  }

  return true;
}

std::vector<CellStream> FilteredInMemoryTableStream::CreateCellStreams(
    std::vector<std::unique_ptr<AbstractCellStreamImpl>> cf_streams) {
  std::vector<CellStream> res;
  res.reserve(cf_streams.size());
  for (auto& stream : cf_streams) {
    res.emplace_back(std::move(stream));
  }
  return res;
}

bool FilteredPersistentTableStream::ApplyFilter(
    InternalFilter const& internal_filter) {
  // TODO: Implement
  return true;
}

std::vector<CellStream> FilteredPersistentTableStream::CreateCellStreams(
    std::vector<std::unique_ptr<AbstractCellStreamImpl>> cf_streams) {
  std::vector<CellStream> res;
  res.reserve(cf_streams.size());
  for (auto& stream : cf_streams) {
    res.emplace_back(std::move(stream));
  }
  return res;
}

StatusOr<StringRangeSet> CreateStringRangeSet(
    google::bigtable::v2::RowSet const& row_set) {
  StringRangeSet res;
  for (auto const& row_key : row_set.row_keys()) {
    if (row_key.size() > kMaxRowLen) {
      return InvalidArgumentError(
          "The row_key in row_set is longer than 4KiB",
          GCP_ERROR_INFO()
              .WithMetadata("row_key size",
                            absl::StrFormat("%zu", row_key.size()))
              .WithMetadata("row_set", row_set.DebugString()));
    }

    if (row_key.empty()) {
      return InvalidArgumentError(
          "`row_key` empty",
          GCP_ERROR_INFO().WithMetadata("row_set", row_set.DebugString()));
    }
    res.Sum(StringRangeSet::Range(row_key, false, row_key, false));
  }
  for (auto const& row_range : row_set.row_ranges()) {
    auto maybe_range = StringRangeSet::Range::FromRowRange(row_range);
    if (!maybe_range) {
      return maybe_range.status();
    }
    if (maybe_range->IsEmpty()) {
      continue;
    }
    res.Sum(*std::move(maybe_range));
  }
  return res;
}

StatusOr<google::bigtable::v2::CheckAndMutateRowResponse>
Table::CheckAndMutateRow(
    google::bigtable::v2::CheckAndMutateRowRequest const& request) {
  std::lock_guard<std::mutex> lock(mu_);

  auto const& row_key = request.row_key();

  if (row_key.size() > kMaxRowLen) {
    return InvalidArgumentError(
        "The row_key is longer than 4KiB",
        GCP_ERROR_INFO()
            .WithMetadata("row_key size",
                          absl::StrFormat("%zu", row_key.size()))
            .WithMetadata("CheckAndMutateRequest", request.DebugString()));
  }

  if (row_key.empty()) {
    return InvalidArgumentError(
        "row key required",
        GCP_ERROR_INFO().WithMetadata("CheckAndMutateRowRequest",
                                      request.DebugString()));
  }

  if (request.true_mutations_size() == 0 &&
      request.false_mutations_size() == 0) {
    return InvalidArgumentError(
        "both true mutations and false mutations are empty",
        GCP_ERROR_INFO().WithMetadata("CheckAndMutateRowRequest",
                                      request.DebugString()));
  }

  auto range_set = std::make_shared<StringRangeSet>();
  range_set->Sum(StringRangeSet::Range(row_key, false, row_key, false));

  StatusOr<CellStream> maybe_stream;
  if (request.has_predicate_filter()) {
    maybe_stream = utilities_->CreateCellStream(
        range_set, std::move(request.predicate_filter()));
  } else {
    maybe_stream = utilities_->CreateCellStream(range_set, absl::nullopt);
  }

  if (!maybe_stream) {
    return maybe_stream.status();
  }

  bool a_cell_is_found = false;

  CellStream& stream = *maybe_stream;
  if (stream) {  // At least one cell/value found when filter is applied
    a_cell_is_found = true;
  }

  Status status;
  if (a_cell_is_found) {
    status = DoMutationsWithPossibleRollback(request.row_key(),
                                             request.true_mutations());
  } else {
    status = DoMutationsWithPossibleRollback(request.row_key(),
                                             request.false_mutations());
  }

  if (!status.ok()) {
    return status;
  }

  google::bigtable::v2::CheckAndMutateRowResponse success_response;
  success_response.set_predicate_matched(a_cell_is_found);

  return success_response;
}

// TODO: Filters for Persistency
Status Table::ReadRows(google::bigtable::v2::ReadRowsRequest const& request,
                       RowStreamer& row_streamer) const {
  std::shared_ptr<StringRangeSet> row_set;
  // We need to check that, not only do we have rows, but that it is
  // not empty (i.e. at least one of row_range or rows is specified).
  if (request.has_rows() && (request.rows().row_ranges_size() > 0 ||
                             request.rows().row_keys_size() > 0)) {
    auto maybe_row_set = CreateStringRangeSet(request.rows());
    if (!maybe_row_set) {
      return maybe_row_set.status();
    }

    row_set = std::make_shared<StringRangeSet>(*std::move(maybe_row_set));
  } else {
    row_set = std::make_shared<StringRangeSet>(StringRangeSet::All());
  }
  std::lock_guard<std::mutex> lock(mu_);

  StatusOr<CellStream> maybe_stream;
  if (request.has_filter()) {
    maybe_stream =
        utilities_->CreateCellStream(row_set, std::move(request.filter()));
  } else {
    maybe_stream = utilities_->CreateCellStream(row_set, absl::nullopt);
  }

  if (!maybe_stream) {
    return maybe_stream.status();
  }

  std::int64_t rows_count = 0;
  absl::optional<std::string> current_row_key;

  CellStream& stream = *maybe_stream;
  for (; stream; ++stream) {
    std::cout << "Row: " << stream->row_key()
              << " column_family: " << stream->column_family()
              << " column_qualifier: " << stream->column_qualifier()
              << " column_timestamp: " << stream->timestamp().count()
              << " column_value: " << stream->value() << " label: "
              << (stream->HasLabel() ? stream->label() : std::string("unset"))
              << std::endl;

    if (request.rows_limit() > 0) {
      if (!current_row_key.has_value() ||
          stream->row_key() != current_row_key.value()) {
        rows_count++;
        current_row_key = stream->row_key();
      }

      if (rows_count > request.rows_limit()) {
        break;
      }
    }

    if (!row_streamer.Stream(*stream)) {
      std::cout << "HOW?" << std::endl;
      return AbortedError("Stream closed by the client.", GCP_ERROR_INFO());
    }
  }

  if (!row_streamer.Flush(true)) {
    std::cout << "Flush failed?" << std::endl;
    return AbortedError("Stream closed by the client.", GCP_ERROR_INFO());
  }
  std::cout << "Print stop" << std::endl;
  return Status();
}

bool Table::IsDeleteProtected() const {
  std::lock_guard<std::mutex> lock(mu_);
  return IsDeleteProtectedNoLock();
}

bool Table::IsDeleteProtectedNoLock() const {
  return schema_.deletion_protection();
}

Status Table::DropRowRange(
    ::google::bigtable::admin::v2::DropRowRangeRequest const& request) {
  std::lock_guard<std::mutex> lock(mu_);

  if (!request.has_row_key_prefix() &&
      !request.has_delete_all_data_from_table()) {
    return InvalidArgumentError(
        "Neither row prefix nor deleted all data from table is set",
        GCP_ERROR_INFO().WithMetadata("DropRowRange request",
                                      request.DebugString()));
  }

  if (request.has_delete_all_data_from_table()) {
    utilities_->RemoveAllDataFromColumnFamilies();
    return Status();
  }

  auto const& row_key_prefix = request.row_key_prefix();
  if (request.row_key_prefix().size() > kMaxRowLen) {
    return InvalidArgumentError(
        "The row_key_prefix is longer than 4KiB",
        GCP_ERROR_INFO().WithMetadata(
            "row_key_prefix size",
            absl::StrFormat("%zu", request.row_key_prefix().size())));
  }
  if (row_key_prefix.empty()) {
    return InvalidArgumentError(
        "Row prefix provided is empty.",
        GCP_ERROR_INFO().WithMetadata("DropRowRange request",
                                      request.DebugString()));
  }

  utilities_->DropRowRange(row_key_prefix);
  return Status();
}

StatusOr<::google::bigtable::v2::ReadModifyWriteRowResponse>
Table::ReadModifyWriteRow(
    google::bigtable::v2::ReadModifyWriteRowRequest const& request) {
  if (request.row_key().size() > kMaxRowLen) {
    return InvalidArgumentError(
        "The row_key is longer than 4KiB",
        GCP_ERROR_INFO().WithMetadata(
            "row_key size", absl::StrFormat("%zu", request.row_key().size())));
  }

  std::lock_guard<std::mutex> lock(mu_);

  std::unique_ptr<RowTransaction> row_transaction =
      utilities_->NewRowTransaction(this->get(), request.row_key());

  auto maybe_response = row_transaction->ReadModifyWriteRow(request);
  if (!maybe_response) {
    return maybe_response.status();
  }

  row_transaction->commit();

  return std::move(maybe_response.value());
}

// NOLINTBEGIN(readability-convert-member-functions-to-static)
Status RowTransaction::AddToCell(
    ::google::bigtable::v2::Mutation_AddToCell const& add_to_cell,
    absl::optional<std::chrono::milliseconds> timestamp_override) {
  auto status = utilities_->FindColumnFamily(add_to_cell);
  if (!status.ok()) {
    return status.status();
  }

  auto cf = status.value();
  auto cf_value_type = cf->GetValueType();
  if (!cf_value_type.has_value() ||
      !cf_value_type.value().has_aggregate_type()) {
    return InvalidArgumentError(
        "column family is not configured to contain aggregation cells or "
        "aggregation type not properly configured",
        GCP_ERROR_INFO().WithMetadata("column family",
                                      add_to_cell.family_name()));
  }

  // Ensure that we support the aggregation that is configured in the
  // column family.
  switch (cf_value_type.value().aggregate_type().aggregator_case()) {
    case google::bigtable::admin::v2::Type::Aggregate::kSum:
    case google::bigtable::admin::v2::Type::Aggregate::kMin:
    case google::bigtable::admin::v2::Type::Aggregate::kMax:
      break;
    default:
      return UnimplementedError(
          "column family configured with unimplemented aggregation",
          GCP_ERROR_INFO()
              .WithMetadata("column family", add_to_cell.family_name())
              .WithMetadata("configured aggregation",
                            absl::StrFormat("%d", cf_value_type.value()
                                                      .aggregate_type()
                                                      .aggregator_case())));
  }

  if (!add_to_cell.has_input()) {
    return InvalidArgumentError(
        "input not set",
        GCP_ERROR_INFO().WithMetadata("mutation", add_to_cell.DebugString()));
  }

  switch (add_to_cell.input().kind_case()) {
    case google::bigtable::v2::Value::kIntValue:
      if (!add_to_cell.input().has_int_value()) {
        return InvalidArgumentError("input value not set",
                                    GCP_ERROR_INFO().WithMetadata(
                                        "mutation", add_to_cell.DebugString()));
      }
      break;
    default:
      return InvalidArgumentError(
          "only int64 values are supported",
          GCP_ERROR_INFO().WithMetadata("mutation", add_to_cell.DebugString()));
  }
  auto int64_input = add_to_cell.input().int_value();

  auto value = google::cloud::internal::EncodeBigEndian(int64_input);

  std::chrono::milliseconds ts_ms;
  if (timestamp_override.has_value()) {
    ts_ms = timestamp_override.value();
  } else {
    ts_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::microseconds(
            add_to_cell.timestamp().raw_timestamp_micros()));
  }

  if (!add_to_cell.has_column_qualifier() ||
      !add_to_cell.column_qualifier().has_raw_value()) {
    return InvalidArgumentError(
        "column qualifier not set",
        GCP_ERROR_INFO().WithMetadata("mutation", add_to_cell.DebugString()));
  }

  return PerformAddToCell(add_to_cell, std::move(cf), ts_ms, value);
}

Status InMemoryRowTransaction::PerformAddToCell(
    ::google::bigtable::v2::Mutation_AddToCell const& add_to_cell,
    std::shared_ptr<ColumnFamily> cf, std::chrono::milliseconds ts_ms,
    std::string& value) {
  auto column_qualifier = add_to_cell.column_qualifier().raw_value();
  auto maybe_old_value =
      cf->UpdateCell(row_key_, column_qualifier, ts_ms, value);
  if (!maybe_old_value) {
    return maybe_old_value.status();
  }

  if (!maybe_old_value.value()) {
    DeleteValue delete_value{*cf, std::move(column_qualifier), ts_ms};
    undo_.emplace(std::move(delete_value));
  } else {
    RestoreValue restore_value{*cf, std::move(column_qualifier), ts_ms,
                               std::move(maybe_old_value.value().value())};
    undo_.emplace(std::move(restore_value));
  }
  return Status();
}

Status RowTransaction::MergeToCell(
    ::google::bigtable::v2::Mutation_MergeToCell const& merge_to_cell) {
  return UnimplementedError(
      "Unsupported mutation type.",
      GCP_ERROR_INFO().WithMetadata("mutation", merge_to_cell.DebugString()));
}

std::string RowTransaction::prepare_key(std::string const& column_qualifier,
                                        int64_t ts) const {
  int64_t mirror = std::numeric_limits<int64_t>::max() - ts;
  return row_key_ + ':' + column_qualifier + ':' +
         absl::StrFormat("%016x", mirror);
}

std::string RowTransaction::prepare_partial_key(
    std::string const& column_qualifier) const {
  return row_key_ + ':' + column_qualifier;
}
// NOLINTEND(readability-convert-member-functions-to-static)

Status InMemoryRowTransaction::DeleteFromColumn(
    ::google::bigtable::v2::Mutation_DeleteFromColumn const&
        delete_from_column) {
  auto maybe_column_family = utilities_->FindColumnFamily(delete_from_column);
  if (!maybe_column_family.ok()) {
    return maybe_column_family.status();
  }

  // We need to check if the given timerange is empty or reversed, but
  // only up to the server's time accuracy (in our case, milliseconds)
  // - For example a time range of [1000, 1200] would be empty.
  if (delete_from_column.has_time_range()) {
    auto start = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::microseconds(
            delete_from_column.time_range().start_timestamp_micros()));
    auto end = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::microseconds(
            delete_from_column.time_range().end_timestamp_micros()));

    // An end timestamp micros of 0 is to be interpreted as infinity,
    // so we allow that.
    if (end <= start &&
        delete_from_column.time_range().end_timestamp_micros() != 0) {
      return InvalidArgumentError(
          "empty or reversed time range: the end timestamp must be more than "
          "the start timestamp when they are truncated to the server's time "
          "precision (milliseconds)",
          GCP_ERROR_INFO().WithMetadata("delete_from_column proto",
                                        delete_from_column.DebugString()));
    }
  }

  auto column_family = maybe_column_family.value();

  auto deleted_cells = column_family->DeleteColumn(
      row_key_, delete_from_column.column_qualifier(),
      delete_from_column.time_range());

  for (auto& cell : deleted_cells) {
    RestoreValue restore_value{
        *column_family, delete_from_column.column_qualifier(),
        std::move(cell.timestamp), std::move(cell.value)};
    undo_.emplace(std::move(restore_value));
  }

  return Status();
}

Status InMemoryRowTransaction::DeleteFromRow() {
  bool row_existed = false;
  for (auto column_family = utilities_->begin();
       column_family != utilities_->end(); ++column_family) {
    auto deleted_columns = column_family->second->DeleteRow(row_key_);

    for (auto& column : deleted_columns) {
      for (auto& cell : column.second) {
        RestoreValue restore_value = {*column_family->second,
                                      std::move(column.first), cell.timestamp,
                                      std::move(cell.value)};
        undo_.emplace(std::move(restore_value));
        row_existed = true;
      }
    }
  }

  if (row_existed) {
    return Status();
  }

  return NotFoundError("row not found in table",
                       GCP_ERROR_INFO().WithMetadata("row", row_key_));
}

Status InMemoryRowTransaction::DeleteFromFamily(
    ::google::bigtable::v2::Mutation_DeleteFromFamily const&
        delete_from_family) {
  // If the request references an incorrect schema (non-existent
  // column family) then return a failure status error immediately.
  auto maybe_column_family = utilities_->FindColumnFamily(delete_from_family);
  if (!maybe_column_family.ok()) {
    return maybe_column_family.status();
  }

  auto column_family_it = utilities_->find(delete_from_family.family_name());
  if (column_family_it == utilities_->end()) {
    return NotFoundError(
        "column family not found in table",
        GCP_ERROR_INFO().WithMetadata("column family",
                                      delete_from_family.family_name()));
  }

  auto ccf = column_family_it->second;
  if (!ccf->RowKeyExists(row_key_)) {
    // The row does not exist
    return NotFoundError(
        "row key is not found in column family",
        GCP_ERROR_INFO()
            .WithMetadata("row key", row_key_)
            .WithMetadata("column family", column_family_it->first));
  }

  auto deleted = column_family_it->second->DeleteRow(row_key_);
  for (auto const& column : deleted) {
    for (auto const& cell : column.second) {
      RestoreValue restore_value{*column_family_it->second,
                                 std::move(column.first), cell.timestamp,
                                 std::move(cell.value)};
      undo_.emplace(std::move(restore_value));
    }
  }

  return Status();
}

// timestamp_override, if provided, will be used instead of
// set_cell.timestamp. The override is used to set the timestamp to
// the server time in case a timestamp <= 0 is provided.
Status InMemoryRowTransaction::SetCell(
    ::google::bigtable::v2::Mutation_SetCell const& set_cell,
    absl::optional<std::chrono::milliseconds> timestamp_override) {
  auto maybe_column_family = utilities_->FindColumnFamily(set_cell);
  if (!maybe_column_family) {
    return maybe_column_family.status();
  }

  auto column_family = maybe_column_family.value();

  auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::microseconds(set_cell.timestamp_micros()));

  if (timestamp_override.has_value()) {
    timestamp = timestamp_override.value();
  }

  auto maybe_old_value = column_family->SetCell(
      row_key_, set_cell.column_qualifier(), timestamp, set_cell.value());

  if (!maybe_old_value) {
    DeleteValue delete_value{*column_family,
                             std::move(set_cell.column_qualifier()), timestamp};
    undo_.emplace(std::move(delete_value));
  } else {
    RestoreValue restore_value{*column_family,
                               std::move(set_cell.column_qualifier()),
                               timestamp, std::move(maybe_old_value.value())};
    undo_.emplace(std::move(restore_value));
  }

  return Status();
}

Status PersistentRowTransaction::commit() {
  rocksdb::Status status = txn_->Commit();
  if (!status.ok()) {
    txn_->Rollback();
    return InvalidArgumentError(
        "Failed to commit the transaction: " + status.ToString(),
        GCP_ERROR_INFO().WithMetadata("row key", row_key_));
  }
  return Status();
}

Status PersistentRowTransaction::SetCell(
    ::google::bigtable::v2::Mutation_SetCell const& set_cell,
    absl::optional<std::chrono::milliseconds> timestamp_override) {
  auto maybe_column_family = utilities_->FindColumnFamily(set_cell);
  if (!maybe_column_family) {
    return maybe_column_family.status();
  }

  auto column_family = maybe_column_family.value();

  auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::microseconds(set_cell.timestamp_micros()));

  if (timestamp_override.has_value()) {
    timestamp = timestamp_override.value();
  }

  std::cout << "Writing to column family: "
            << column_family->GetRaw()->GetName() << "; row key: " << row_key_
            << "; column: " << set_cell.column_qualifier()
            << "; value: " << set_cell.value() << std::endl;

  std::string prepared_key =
      prepare_key(set_cell.column_qualifier(), timestamp.count());
  rocksdb::Status status =
      txn_->Put(column_family->GetRaw(), prepared_key, set_cell.value());

  if (!status.ok()) {
    return InternalError(
        "Failed to put write into the transaction; " + status.ToString(),
        GCP_ERROR_INFO().WithMetadata("column_family",
                                      column_family->GetRaw()->GetName()));
  }
  return Status();
}

Status PersistentRowTransaction::PerformAddToCell(
    ::google::bigtable::v2::Mutation_AddToCell const& add_to_cell,
    std::shared_ptr<ColumnFamily> cf, std::chrono::milliseconds ts_ms,
    std::string& value) {
  rocksdb::ColumnFamilyHandle* raw_cf = cf->GetRaw();

  std::string partial_key =
      prepare_partial_key(add_to_cell.column_qualifier().raw_value());
  rocksdb::Endpoint start(partial_key, true);
  rocksdb::Endpoint end(partial_key, true);
  rocksdb::Status status = txn_->GetRangeLock(raw_cf, start, end);
  if (!status.ok()) {
    return InternalError(
        "Failed to add to cell: " + status.ToString(),
        GCP_ERROR_INFO().WithMetadata("mutation", add_to_cell.DebugString()));
  }

  rocksdb::Iterator* it = txn_->GetIterator(rocksdb::ReadOptions(), raw_cf);
  it->Seek(partial_key);
  std::string new_value = value;
  if (it->Valid() && it->key().starts_with(partial_key)) {
    auto maybe_result =
        cf->update_cell_(it->value().ToString(), std::move(value));
    if (!maybe_result) {
      return InternalError(
          "Failed to add to cell: " + maybe_result.status().message(),
          GCP_ERROR_INFO().WithMetadata("mutation", add_to_cell.DebugString()));
    }
    new_value = maybe_result.value();
  }

  std::string new_key =
      prepare_key(add_to_cell.column_qualifier().raw_value(), ts_ms.count());
  status = txn_->Put(raw_cf, new_key, std::move(new_value));
  if (!status.ok()) {
    return InternalError(
        "Failed to add to cell: " + status.ToString(),
        GCP_ERROR_INFO().WithMetadata("mutation", add_to_cell.DebugString()));
  }
  if (it->Valid() && it->key().starts_with(partial_key)) {
    status = txn_->Delete(raw_cf, it->key());
    if (!status.ok()) {
      return InternalError(
          "Failed to add to cell: " + status.ToString(),
          GCP_ERROR_INFO().WithMetadata("mutation", add_to_cell.DebugString()));
    }
  }

  return Status();
}

Status PersistentRowTransaction::DeleteFromColumn(
    ::google::bigtable::v2::Mutation_DeleteFromColumn const&
        delete_from_column) {
  // TODO: Implement
  return InternalError("UNIMPLEMENTED", GCP_ERROR_INFO());
}

Status PersistentRowTransaction::DeleteFromRow() {
  // TODO: Implement
  return InternalError("UNIMPLEMENTED", GCP_ERROR_INFO());
}

Status PersistentRowTransaction::DeleteFromFamily(
    ::google::bigtable::v2::Mutation_DeleteFromFamily const&
        delete_from_family) {
  // TODO: Implement
  return InternalError("UNIMPLEMENTED", GCP_ERROR_INFO());
}

StatusOr<::google::bigtable::v2::ReadModifyWriteRowResponse>
PersistentRowTransaction::ReadModifyWriteRow(
    google::bigtable::v2::ReadModifyWriteRowRequest const& request) {
  return InternalError("UNIMPLEMENTED", GCP_ERROR_INFO());
}

// ProcessReadModifyWriteRuleResult records the result of a
// ReadModifyWriteRule computation for possible undo in the undo log
// and also updates the tmp_families temporary table (containing only
// one row) with the modified cell for later return.
void ProcessReadModifyWriteResult(
    std::shared_ptr<InMemoryColumnFamily>& column_family,
    std::string const& row_key,
    std::stack<absl::variant<DeleteValue, RestoreValue>>& undo,
    google::bigtable::v2::ReadModifyWriteRule const& rule,
    ReadModifyWriteCellResult& result,
    std::map<std::string, InMemoryColumnFamily>& tmp_families) {
  if (result.maybe_old_value.has_value()) {
    // We overwrote a cell, we need to record a RestoreValue in the undo log
    RestoreValue restore_value{*column_family, rule.column_qualifier(),
                               result.timestamp,
                               std::move(result.maybe_old_value.value())};
    undo.emplace(std::move(restore_value));
  } else {
    // We created a new cell -- we would need to delete it in any rollback
    DeleteValue delete_value{*column_family, rule.column_qualifier(),
                             result.timestamp};
    undo.emplace(std::move(delete_value));
  }

  // Record the cell in our local mini table here to use in
  // assembling a row of changed cells for return.
  tmp_families[rule.family_name()].SetCell(row_key, rule.column_qualifier(),
                                           result.timestamp,
                                           std::move(result.value));
}

google::bigtable::v2::ReadModifyWriteRowResponse
FamiliesToReadModifyWriteResponse(
    std::string const& row_key,
    std::map<std::string, InMemoryColumnFamily> const& families) {
  google::bigtable::v2::ReadModifyWriteRowResponse resp;
  auto* row = resp.mutable_row();
  row->set_key(row_key);

  for (auto const& fam : families) {
    auto* family = row->add_families();
    family->set_name(fam.first);
    for (auto const& r : fam.second) {
      for (auto const& cfr : r.second) {
        auto* col = family->add_columns();
        col->set_qualifier(cfr.first);
        for (auto const& cr : cfr.second) {
          auto* cell = col->add_cells();
          cell->set_timestamp_micros(
              std::chrono::duration_cast<std::chrono::microseconds>(cr.first)
                  .count());
          cell->set_value(std::move(cr.second));
        }
      }
    }
  }

  return resp;
}

StatusOr<::google::bigtable::v2::ReadModifyWriteRowResponse>
InMemoryRowTransaction::ReadModifyWriteRow(
    google::bigtable::v2::ReadModifyWriteRowRequest const& request) {
  if (row_key_.empty()) {
    return InvalidArgumentError(
        "row key not set",
        GCP_ERROR_INFO().WithMetadata("request", request.DebugString()));
  }

  // tmp_families is a small one row mini table used to accumulate
  // changed cells efficiently for later return in the row returned by
  // the RPC.
  std::map<std::string, InMemoryColumnFamily> tmp_families;

  for (auto const& rule : request.rules()) {
    auto maybe_column_family = utilities_->FindColumnFamily(rule);
    if (!maybe_column_family) {
      return maybe_column_family.status();
    }

    // For now, let's keep this. Not sure how this will look for the Persistent
    // CF yet (RocksDB has Merge operators), so I don't think it makes sense to
    // make ReadModifyWrite A part of the ColumnFamily API.
    auto column_family = std::static_pointer_cast<InMemoryColumnFamily>(
        maybe_column_family.value());

    if (rule.has_append_value()) {
      auto result = column_family->ReadModifyWrite(
          row_key_, rule.column_qualifier(), rule.append_value());

      ProcessReadModifyWriteResult(column_family, row_key_, undo_, rule, result,
                                   tmp_families);

    } else if (rule.has_increment_amount()) {
      auto maybe_result = column_family->ReadModifyWrite(
          row_key_, rule.column_qualifier(), rule.increment_amount());
      if (!maybe_result) {
        return maybe_result.status();
      }

      auto& result = maybe_result.value();

      ProcessReadModifyWriteResult(column_family, row_key_, undo_, rule, result,
                                   tmp_families);

    } else {
      return InvalidArgumentError(
          "either append value or increment amount must be set",
          GCP_ERROR_INFO().WithMetadata("rule", rule.DebugString()));
    }
  }

  // Now assemble the returned value.
  return FamiliesToReadModifyWriteResponse(row_key_, tmp_families);
}

void InMemoryRowTransaction::Undo() {
  auto row_key = row_key_;

  while (!undo_.empty()) {
    auto op = undo_.top();
    undo_.pop();

    auto* restore_value = absl::get_if<RestoreValue>(&op);
    if (restore_value) {
      restore_value->column_family.SetCell(
          row_key, std::move(restore_value->column_qualifier),
          restore_value->timestamp, std::move(restore_value->value));
      continue;
    }

    auto* delete_value = absl::get_if<DeleteValue>(&op);
    if (delete_value) {
      delete_value->column_family.DeleteTimeStamp(
          row_key, std::move(delete_value->column_qualifier),
          delete_value->timestamp);
      continue;
    }

    // If we get here, there is a type of undo log that has not been
    // implemented!
    std::abort();
  }
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
