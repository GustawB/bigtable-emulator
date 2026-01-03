// utils.h
#include "google/cloud/status.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include <memory>
#include <string>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

class ColumnFamily;
class PersistentColumnFamily;
class ModifyCfRollback {
 public:
  explicit ModifyCfRollback(std::shared_ptr<rocksdb::DB> db)
      : db_(std::move(db)) {}

  void RecordCreated(std::string id,
                     std::shared_ptr<rocksdb::ColumnFamilyHandle> handle);

  void RecordDropped(std::string id,
                     std::shared_ptr<PersistentColumnFamily> cf_obj,
                     std::shared_ptr<rocksdb::ColumnFamilyHandle> old_handle,
                     rocksdb::ColumnFamilyDescriptor desc);

  void Commit();
  Status Rollback();

 private:
  using CfHandlePtr = std::shared_ptr<rocksdb::ColumnFamilyHandle>;

  CfHandlePtr AdoptHandle(rocksdb::ColumnFamilyHandle* raw);

  Status CopyAllKeys(CfHandlePtr const& from, CfHandlePtr const& to);

  std::shared_ptr<rocksdb::DB> db_;

  struct DroppedCfBackup {
    std::string id;
    std::shared_ptr<PersistentColumnFamily> cf_obj;
    CfHandlePtr old_handle;
    rocksdb::ColumnFamilyDescriptor desc;
  };

  struct CreatedCfBackup {
    std::string id;
    CfHandlePtr handle;
  };

  std::vector<CreatedCfBackup> created_;
  std::vector<DroppedCfBackup> dropped_;
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
