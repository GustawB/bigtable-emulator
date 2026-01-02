#include "utils.h"
#include "google/cloud/internal/make_status.h"
#include "column_family.h"
#include "rocksdb/iterator.h"
#include "rocksdb/write_batch.h"

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

void ModifyCfRollback::RecordCreated(
    std::string id, std::shared_ptr<rocksdb::ColumnFamilyHandle> handle) {
  created_.push_back(CreatedCfBackup{std::move(id), std::move(handle)});
}

void ModifyCfRollback::RecordDropped(
    std::string id, std::shared_ptr<PersistentColumnFamily> cf_obj,
    std::shared_ptr<rocksdb::ColumnFamilyHandle> old_handle,
    rocksdb::ColumnFamilyDescriptor desc) {
  dropped_.push_back(DroppedCfBackup{std::move(id), std::move(cf_obj),
                                     std::move(old_handle), std::move(desc)});
}

void ModifyCfRollback::Commit() {
  created_.clear();
  dropped_.clear();
}

ModifyCfRollback::CfHandlePtr ModifyCfRollback::AdoptHandle(
    rocksdb::DB* db, rocksdb::ColumnFamilyHandle* raw) {
  return CfHandlePtr(raw, [db](rocksdb::ColumnFamilyHandle* h) {
    if (h == nullptr) return;
    (void)db->DestroyColumnFamilyHandle(h);
  });
}

Status ModifyCfRollback::Rollback() {
  for (auto it = created_.rbegin(); it != created_.rend(); ++it) {
    if (!it->handle) continue;

    rocksdb::Status s = db_->DropColumnFamily(it->handle.get());
    if (!s.ok()) {
      return InternalError(
          "Rollback: DropColumnFamily(created) failed: " + s.ToString(),
          GCP_ERROR_INFO().WithMetadata("cf", it->id));
    }

    it->handle.reset();
  }

  for (auto it = dropped_.rbegin(); it != dropped_.rend(); ++it) {
    rocksdb::ColumnFamilyHandle* raw = nullptr;
    rocksdb::Status s =
        db_->CreateColumnFamily(it->desc.options, it->desc.name, &raw);
    if (!s.ok()) {
      return InternalError(
          "Rollback: CreateColumnFamily(dropped) failed: " + s.ToString(),
          GCP_ERROR_INFO().WithMetadata("cf", it->id));
    }

    auto new_handle = AdoptHandle(db_, raw);

    Status copy = CopyAllKeys(it->old_handle, new_handle);
    if (!copy.ok()) {
      (void)db_->DropColumnFamily(new_handle.get());
      return copy;
    }

    it->cf_obj->ResetHandle(new_handle);
  }

  return Status();
}

Status ModifyCfRollback::CopyAllKeys(CfHandlePtr const& from,
                                     CfHandlePtr const& to) {
  rocksdb::ReadOptions ro;
  ro.fill_cache = false;

  rocksdb::WriteOptions wo;

  std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(ro, from.get()));
  rocksdb::WriteBatch batch;

  constexpr std::size_t kMaxBatchBytes = 4 * 1024 * 1024;
  std::size_t approx_bytes = 0;

  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    auto k = it->key();
    auto v = it->value();

    batch.Put(to.get(), k, v);
    approx_bytes += k.size() + v.size();

    if (approx_bytes >= kMaxBatchBytes) {
      rocksdb::Status s = db_->Write(wo, &batch);
      if (!s.ok()) {
        return InternalError("Rollback: Write(batch) failed: " + s.ToString(),
                             GCP_ERROR_INFO());
      }
      batch.Clear();
      approx_bytes = 0;
    }
  }

  if (rocksdb::Status s = it->status(); !s.ok()) {
    return InternalError("Rollback: iterator status failed: " + s.ToString(),
                         GCP_ERROR_INFO());
  }

  if (batch.Count() > 0) {
    rocksdb::Status s = db_->Write(wo, &batch);
    if (!s.ok()) {
      return InternalError(
          "Rollback: Write(final batch) failed: " + s.ToString(),
          GCP_ERROR_INFO());
    }
  }

  return Status();
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
