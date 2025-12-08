#include "merge_operator.h"
#include "google/cloud/internal/big_endian.h"
#include <rocksdb/env.h>
#include <algorithm>
#include <limits>
#include <optional>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {
namespace {
template <typename Op>
bool MergeCell(rocksdb::Slice const* existing_value,
               rocksdb::Slice const& value,
               std::string* new_value,
               rocksdb::Logger* logger,
               int64_t default_if_missing,
               Op operation) {

  int64_t existing_int = default_if_missing;

  if (existing_value) {
    auto maybe_existing = google::cloud::internal::DecodeBigEndian<std::int64_t>(
        existing_value->ToString());
    if (!maybe_existing) {
      rocksdb::Log(logger, "Failed to decode current value during merge");
      return false;
    }
    existing_int = maybe_existing.value();
  }

  auto maybe_new = google::cloud::internal::DecodeBigEndian<std::int64_t>(
      value.ToString());
  if (!maybe_new) {
    rocksdb::Log(logger, "Failed to decode new value during merge");
    return false;
  }

  int64_t result = operation(existing_int, maybe_new.value());
  *new_value = google::cloud::internal::EncodeBigEndian(result);

  return true;
}

} // anonymous namespace

bool SumUpdateCellBEInt64::Merge(rocksdb::Slice const& key,
                                 rocksdb::Slice const* existing_value,
                                 rocksdb::Slice const& value,
                                 std::string* new_value,
                                 rocksdb::Logger* logger) const {
  return MergeCell(existing_value, value, new_value, logger,
                   0,
                   std::plus<>());
}

bool MaxUpdateCellBEInt64::Merge(rocksdb::Slice const& key,
                                 rocksdb::Slice const* existing_value,
                                 rocksdb::Slice const& value,
                                 std::string* new_value,
                                 rocksdb::Logger* logger) const {
  return MergeCell(existing_value, value, new_value, logger,
                   std::numeric_limits<int64_t>::min(),
                   [](int64_t a, int64_t b) { return std::max(a, b); });
}

bool MinUpdateCellBEInt64::Merge(rocksdb::Slice const& key,
                                 rocksdb::Slice const* existing_value,
                                 rocksdb::Slice const& value,
                                 std::string* new_value,
                                 rocksdb::Logger* logger) const {
  return MergeCell(existing_value, value, new_value, logger,
                   std::numeric_limits<int64_t>::max(),
                   [](int64_t a, int64_t b) { return std::min(a, b); });
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google