#include "merge_operator.h"
#include "google/cloud/internal/big_endian.h"
#include <rocksdb/env.h>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

bool SumUpdateCellBEInt64::Merge(rocksdb::Slice const& key,
                                 rocksdb::Slice const* existing_value,
                                 rocksdb::Slice const& value,
                                 std::string* new_value,
                                 rocksdb::Logger* logger) const {
  int64_t existing_value_int = 0;
  if (existing_value) {
    auto maybe_existing_value_int =
        google::cloud::internal::DecodeBigEndian<std::int64_t>(
            existing_value->ToString());
    if (!maybe_existing_value_int) {
      rocksdb::Log(logger, ("Failed to decode current value during merge"));
      return false;
    }
    existing_value_int = maybe_existing_value_int.value();
  }

  auto new_value_int =
      google::cloud::internal::DecodeBigEndian<std::int64_t>(value.ToString());
  if (!new_value_int) {
    rocksdb::Log(logger, ("Failed to decode new value during merge"));
    return false;
  }
  *new_value = google::cloud::internal::EncodeBigEndian(existing_value_int +
                                                        new_value_int.value());
  return true;
}

bool MaxUpdateCellBEInt64::Merge(rocksdb::Slice const& key,
                                 rocksdb::Slice const* existing_value,
                                 rocksdb::Slice const& value,
                                 std::string* new_value,
                                 rocksdb::Logger* logger) const {
  auto existing_int = google::cloud::internal::DecodeBigEndian<std::int64_t>(
      existing_value->ToString());
  if (!existing_int) {
    rocksdb::Log(logger, ("Failed to decode current value during merge"));
    return false;
  }
  auto new_int =
      google::cloud::internal::DecodeBigEndian<std::int64_t>(value.ToString());
  if (!new_int) {
    rocksdb::Log(logger, ("Failed to decode new value during merge"));
    return false;
  }

  if (existing_int.value() > new_int.value()) {
    *new_value = existing_value->ToString();
  } else {
    *new_value = google::cloud::internal::EncodeBigEndian(new_int.value());
  }

  return true;
}

bool MinUpdateCellBEInt64::Merge(rocksdb::Slice const& key,
                                 rocksdb::Slice const* existing_value,
                                 rocksdb::Slice const& value,
                                 std::string* new_value,
                                 rocksdb::Logger* logger) const {
  auto existing_int = google::cloud::internal::DecodeBigEndian<std::int64_t>(
      existing_value->ToString());
  if (!existing_int) {
    rocksdb::Log(logger, ("Failed to decode current value during merge"));
    return false;
  }
  auto new_int =
      google::cloud::internal::DecodeBigEndian<std::int64_t>(value.ToString());
  if (!new_int) {
    rocksdb::Log(logger, ("Failed to decode new value during merge"));
    return false;
  }

  if (existing_int.value() < new_int.value()) {
    *new_value = existing_value->ToString();
  } else {
    *new_value = google::cloud::internal::EncodeBigEndian(new_int.value());
  }

  return true;
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google