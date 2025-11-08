#include "timestamp_comparator.h"

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

TimestampComparator::TimestampComparator() : Comparator(sizeof(int64_t)) {}

const char *TimestampComparator::Name() const {
  return "TimestampComparator";
}

int TimestampComparator::Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const {
  return std::stoll(a.ToString()) - std::stoll(b.ToString());
}

rocksdb::Slice TimestampComparator::GetMaxTimestamp() const {
  return rocksdb::Slice(std::to_string(INT64_MAX));
}

rocksdb::Slice TimestampComparator::GetMinTimestamp() const {
  // I'd prefer 0, but technically int64 goes lower
  return rocksdb::Slice(std::to_string(INT64_MIN));
}

std::string TimestampComparator::TimestampToString(const rocksdb::Slice& ts) const {
  return ts.ToString();
}

int TimestampComparator::CompareTimestamp(const rocksdb::Slice& ts1,const rocksdb::Slice& ts2) const {
  return Compare(ts1,ts2);
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google