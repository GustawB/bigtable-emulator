#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_TIMESTAMP_COMPARATOR_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_TIMESTAMP_COMPARATOR_H

#include <rocksdb/comparator.h>
#include <rocksdb/slice.h>
#include <iomanip>
#include <sstream>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {
class TimestampComparator : public rocksdb::Comparator {
 public:
  TimestampComparator()
      : Comparator(2 * sizeof(int64_t)),
        min_(absl::StrFormat("%016x", INT64_MAX)),
        max_(absl::StrFormat("%016x", 0)) {}

  char const* Name() const override { return "TimestampComparator"; }

  int Compare(rocksdb::Slice const& a, rocksdb::Slice const& b) const override {
    /**
     * By default, smaller values will be first, we don't want that. That's why
     * the result is reversed. And that's why in the constructor the max value
     * is zero; We want larger values first.
     */
    return -(a.compare(b));
  }

  void FindShortestSeparator(std::string* start,
                             rocksdb::Slice const& limit) const override {};

  void FindShortSuccessor(std::string* key) const override {};

  bool CanKeysWithDifferentByteContentsBeEqual() const override {
    return false;
  }

  rocksdb::Slice GetMaxTimestamp() const override {
    return rocksdb::Slice(max_);
  }

  rocksdb::Slice GetMinTimestamp() const override {
    return rocksdb::Slice(min_);
  }

  std::string TimestampToString(rocksdb::Slice const& ts) const override {
    return ts.ToString();
  }

  int CompareTimestamp(rocksdb::Slice const& ts1,
                       rocksdb::Slice const& ts2) const override {
    return Compare(ts1, ts2);
  }

 private:
  std::string min_;
  std::string max_;
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_TIMESTAMP_COMPARATOR_H