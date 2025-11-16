#include "merge_operator.h"

#include <rocksdb/env.h>

#include "google/cloud/internal/big_endian.h"

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

bool SumUpdateCellBEInt64::Merge(const rocksdb::Slice &key, const rocksdb::Slice *existing_value,
    const rocksdb::Slice &value, std::string *new_value, rocksdb::Logger *logger) const {
    auto existing_value_int =
        google::cloud::internal::DecodeBigEndian<std::int64_t>(existing_value->ToString());
    if (!existing_value_int) {
        rocksdb::Log(logger, ("Failed to decode current value during merge"));
        return false;
    }

    auto new_value_int =
        google::cloud::internal::DecodeBigEndian<std::int64_t>(value.ToString());
    if (!new_value_int) {
        rocksdb::Log(logger, ("Failed to decode new value during merge"));
        return false;
    }

    *new_value = google::cloud::internal::EncodeBigEndian(existing_value_int.value() +
                                                    new_value_int.value());
    return true;
}

bool MaxUpdateCellBEInt64::Merge(const rocksdb::Slice &key, const rocksdb::Slice *existing_value,
    const rocksdb::Slice &value, std::string *new_value, rocksdb::Logger *logger) const {
    auto existing_int =
        google::cloud::internal::DecodeBigEndian<std::int64_t>(existing_value->ToString());
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
        *new_value = new_int.value();
    }

    return true;
}

bool MinUpdateCellBEInt64::Merge(const rocksdb::Slice &key, const rocksdb::Slice *existing_value,
const rocksdb::Slice &value, std::string *new_value, rocksdb::Logger *logger) const {
    auto existing_int =
        google::cloud::internal::DecodeBigEndian<std::int64_t>(existing_value->ToString());
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
        *new_value = new_int.value();
    }

    return true;
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google