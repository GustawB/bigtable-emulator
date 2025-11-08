#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_TIMESTAMP_COMPARATOR_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_TIMESTAMP_COMPARATOR_H

#include <rocksdb/comparator.h>
#include <rocksdb/slice.h>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

class TimestampComparator : public rocksdb::Comparator {
public:
  TimestampComparator();

  const char* Name() const override;

  int Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const override;

  void FindShortestSeparator(std::string* start, const rocksdb::Slice& limit) const override {};

  void FindShortSuccessor(std::string* key) const override {};

  bool CanKeysWithDifferentByteContentsBeEqual() const override { return false; }

  rocksdb::Slice GetMaxTimestamp() const override;

  rocksdb::Slice GetMinTimestamp() const override;

  std::string TimestampToString(const rocksdb::Slice& ts) const override;

  int CompareTimestamp(const rocksdb::Slice& ts1,const rocksdb::Slice& ts2) const override;
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_TIMESTAMP_COMPARATOR_H