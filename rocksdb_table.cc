#include "rocksdb_table.h"

#include "google/cloud/internal/make_status.h"

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

StatusOr<std::unique_ptr<RocksDBTable>> RocksDBTable::Create(const std::string& table_name, google::bigtable::admin::v2::Table schema) {
  std::unique_ptr<RocksDBTable> table(new RocksDBTable);
  rocksdb::DB* db;
  rocksdb::Options options;
  options.create_if_missing = true;
  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  for (const auto& cfd : schema.column_families()) {
    rocksdb::ColumnFamilyOptions opts;
    column_families.emplace_back(cfd.first, opts);
  }

  rocksdb::Status status = rocksdb::DB::Open(options, "/tmp/" + table_name, column_families, &table->handles_, &db);
  if (!status.ok()) {
    return InternalError(
      "failed to create new rocksdb instance",
      GCP_ERROR_INFO().WithMetadata("schema", schema.DebugString())
      );
  }
  
  table->db_ = db;
  table->schema_ = std::move(schema);
  return table;
}

RocksDBTable::~RocksDBTable() {
  // TODO: think about this
  delete db_;
}
}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google