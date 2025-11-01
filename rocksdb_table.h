#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_ROCKSDB_TABLE_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_ROCKSDB_TABLE_H

#include <string>
#include <rocksdb/db.h>
#include "google/cloud/status_or.h"
#include <google/bigtable/admin/v2/table.pb.h>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

class RocksDBTable {
public:
  ~RocksDBTable();

  static StatusOr<std::unique_ptr<RocksDBTable>> Create(const std::string& table_name, google::bigtable::admin::v2::Table schema);

private:
  RocksDBTable();

  rocksdb::DB* db_;
  google::bigtable::admin::v2::Table schema_;
  std::vector<rocksdb::ColumnFamilyHandle*> handles_;
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif //GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_ROCKSDB_TABLE_H