#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_TIMESTAMP_COMPARATOR_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_TIMESTAMP_COMPARATOR_H

#include <rocksdb/comparator.h>
#include <rocksdb/slice.h>
#include <sstream>
#include <iomanip>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {
class TimestampComparator : public rocksdb::Comparator {
public:
  TimestampComparator()  : Comparator(2 * sizeof(int64_t)),
    min_(absl::StrFormat("%016x", INT64_MIN)), max_(absl::StrFormat("%016x", INT64_MAX)) {}

  const char* Name() const override {
      return "TimestampComparator";
  }

  int Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const override {
      return a.compare(b);
  }

  void FindShortestSeparator(std::string* start, const rocksdb::Slice& limit) const override {};

  void FindShortSuccessor(std::string* key) const override {};

  bool CanKeysWithDifferentByteContentsBeEqual() const override { return false; }

  rocksdb::Slice GetMaxTimestamp() const override {
      return rocksdb::Slice(max_);
  }

  rocksdb::Slice GetMinTimestamp() const override {
      return rocksdb::Slice(min_);
  }

  std::string TimestampToString(const rocksdb::Slice& ts) const override {
      return ts.ToString();
  }

  int CompareTimestamp(const rocksdb::Slice& ts1,const rocksdb::Slice& ts2) const override {
      return Compare(ts1,ts2);
  }

private:
    std::string min_;
    std::string max_;
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_TIMESTAMP_COMPARATOR_H