#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_KEY_CODER_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_KEY_CODER_H

#include "google/cloud/internal/make_status.h"
#include "google/cloud/status_or.h"
#include "absl/strings/str_format.h"
#include <string>

/**
 * KeyCoder class is responsible for API used in encoding and decoding keys for
 * the persistent database. The general format of the key is row#col#timestamp,
 * where # is a separator, and the timestamp has a fixed length of 16 and  has
 * its bits reversed. # is two bytes: "\x00\x01". 0x00 is the "logical"
 * separator. 0x01 is used to distinguish 0x00 added by the encoder from the
 * 0x00 already present in the row or key. For that, any existing 0x00 in the
 * row or col is swapped for "\x00\xFF".
 */
namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

struct DecodeResult {
  std::string row;
  std::string col;
  uint64_t timestamp;
};

class KeyCoder {
 public:
  static std::string Encode(std::string const& row, std::string const& col,
                            uint64_t timestamp) {
    std::string buffer;
    buffer.reserve(row.size() + col.size() + 16);

    AppendEscaped(buffer, row);
    buffer.append("\x00\x01", 2);

    AppendEscaped(buffer, col);
    buffer.append("\x00\x01", 2);

    std::string encoded_ts = absl::StrFormat("%016x", ~timestamp);
    buffer.append(encoded_ts);

    return buffer;
  }

  static std::string PartialEncode(std::string const& row,
                                   std::string const& col) {
    std::string buffer;
    buffer.reserve(row.size() + col.size() + 16);

    AppendEscaped(buffer, row);
    buffer.append("\x00\x01", 2);

    AppendEscaped(buffer, col);
    buffer.append("\x00\x01", 2);

    return buffer;
  }

  static StatusOr<DecodeResult> Decode(std::string const& full_key) {
    size_t pos = 0;
    DecodeResult decode_result;
    auto res = ConsumeField(full_key, pos);
    if (!res.ok()) return res.status();
    decode_result.row = res.value();

    res = ConsumeField(full_key, pos);
    if (!res.ok()) return res.status();
    decode_result.col = res.value();

    if (full_key.size() - pos != 16)
      return InternalError("Received invalid key",
                           GCP_ERROR_INFO().WithMetadata("key", full_key));

    std::string ts_hex = full_key.substr(pos);
    try {
      uint64_t be_ts = std::stoull(ts_hex, nullptr, 16);
      decode_result.timestamp = ~be_ts;
    } catch (...) {
      return InternalError("Failed to parse timestamp hex",
                           GCP_ERROR_INFO().WithMetadata("ts_hex", ts_hex));
    }

    return decode_result;
  }

 private:
  static void AppendEscaped(std::string& dest, std::string const& src) {
    size_t pos = 0;
    while (true) {
      size_t zero_idx = src.find('\0', pos);

      if (zero_idx == absl::string_view::npos) {
        dest.append(src.data() + pos, src.size() - pos);
        break;
      }

      dest.append(src.data() + pos, zero_idx - pos);
      dest.append("\x00\xFF", 2);
      pos = zero_idx + 1;
    }
  }

  static StatusOr<std::string> ConsumeField(std::string const& src,
                                            size_t& pos) {
    std::string res;
    while (pos < src.size()) {
      size_t zero_idx = src.find('\0', pos);
      if (zero_idx == std::string::npos) {
        return InternalError("Failed to decode key",
                             GCP_ERROR_INFO().WithMetadata("key_left", src));
      }

      res.append(src, pos, zero_idx - pos);
      if (zero_idx + 1 >= src.size())
        return InternalError("Failed to decode key",
                             GCP_ERROR_INFO().WithMetadata("key_left", src));
      char next_byte = src[zero_idx + 1];
      if (next_byte == '\xFF') {
        res.push_back('\0');
        pos = zero_idx + 2;
      } else if (next_byte == '\x01') {
        pos = zero_idx + 2;
        return res;
      } else {
        return InternalError("Failed to decode key",
                             GCP_ERROR_INFO().WithMetadata("key_left", src));
      }
    }
    return InternalError("Broken key: loop fell through",
                         GCP_ERROR_INFO().WithMetadata("key_left", src));
  }
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_KEY_CODER_H