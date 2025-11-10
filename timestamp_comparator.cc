#include "timestamp_comparator.h"

#include <sstream>
#include <iomanip>
#include <iostream>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

std::string TimestampToHexString(int64_t timestamp) {
  std::stringstream stream;
  stream << std::setfill ('0') << std::setw(sizeof(int64_t)*2)
       << std::hex << timestamp;
  return stream.str();
}

TimestampComparator::TimestampComparator() : Comparator(2 * sizeof(int64_t)) {}

const char *TimestampComparator::Name() const {
  return "TimestampComparator";
}

int TimestampComparator::Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const {
  std::string a_string = a.ToString();
  std::string b_string = b.ToString();
  int a_separator = a_string.size() - 16;
  int b_separator = b_string.size() - 16;
  std::string a_key = a_string.substr(0, a_separator);
  std::string a_timestamp = a_string.substr(a_separator, a_string.size());
  std::string b_timestamp = b_string.substr(b_separator, b_string.size());
  std::string b_key = b_string.substr(0, b_separator);
  int64_t a_int = std::stoll(a_timestamp, nullptr, 16);
  int64_t b_int = std::stoll(b_timestamp, nullptr, 16);

  if (a_key < b_key) return -1;
  if (a_key > b_key) return 1;
  if (a_int < b_int) return -1;
  if (a_int > b_int) return 1;
  return 0;
}

rocksdb::Slice TimestampComparator::GetMaxTimestamp() const {
  return rocksdb::Slice(TimestampToHexString(INT64_MAX));
}

rocksdb::Slice TimestampComparator::GetMinTimestamp() const {
  // I'd prefer 0, but technically int64 goes lower
  return rocksdb::Slice(TimestampToHexString(INT64_MIN));
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