#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_MERGE_OPERATOR_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_MERGE_OPERATOR_H

#include <rocksdb/merge_operator.h>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

class SumUpdateCellBEInt64 : public rocksdb::AssociativeMergeOperator {
  bool Merge(rocksdb::Slice const& key, rocksdb::Slice const* existing_value,
             rocksdb::Slice const& value, std::string* new_value,
             rocksdb::Logger* logger) const override;

  char const* Name() const override { return "SumUpdateCellBEInt64"; }
};

class MaxUpdateCellBEInt64 : public rocksdb::AssociativeMergeOperator {
  bool Merge(rocksdb::Slice const& key, rocksdb::Slice const* existing_value,
             rocksdb::Slice const& value, std::string* new_value,
             rocksdb::Logger* logger) const override;

  char const* Name() const override { return "MaxUpdateCellBEInt64"; }
};

class MinUpdateCellBEInt64 : public rocksdb::AssociativeMergeOperator {
  bool Merge(rocksdb::Slice const& key, rocksdb::Slice const* existing_value,
             rocksdb::Slice const& value, std::string* new_value,
             rocksdb::Logger* logger) const override;

  char const* Name() const override { return "MinUpdateCellBEInt64"; }
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_MERGE_OPERATOR_H