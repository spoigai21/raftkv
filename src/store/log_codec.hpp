#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <variant>
#include <vector>

#include <tl/expected.hpp>

#include "raft/types.hpp"

// The on-disk log record format (implementation guide Phase 4).
//
//   [len u32][payload_crc u32][header_crc u32][payload: len bytes]      all little-endian
//
// header_crc covers len and payload_crc, so a damaged length is detected as corruption
// rather than mistaken for a record that runs past the end of the file (a torn write).
// The payload is one of:
//   0x01 entry:         [term u64][index u64][command: the remaining bytes]
//   0x02 truncate-from: [index u64]
namespace raftkv::store {

// CRC-32 (IEEE 802.3, reflected, as used by zlib and Ethernet).
std::uint32_t crc32(std::span<const std::byte> data, std::uint32_t crc = 0);

inline constexpr std::size_t kRecordHeaderSize = 12;
inline constexpr std::uint32_t kMaxRecordPayload = 16u << 20;   // implementation guide §3.5

struct TruncateFrom {
    raft::Index index = 0;
    friend bool operator==(const TruncateFrom&, const TruncateFrom&) = default;
};

using LogRecord = std::variant<raft::LogEntry, TruncateFrom>;

enum class DecodeError {
    Truncated,     // fewer bytes than the header, or than the header says: maybe a torn write
    BadHeader,     // the header checksum does not match: the length cannot be trusted
    TooLarge,      // a valid header claiming more than kMaxRecordPayload
    BadChecksum,   // a complete record whose payload checksum does not match
    BadPayload,    // checksums match but the payload is not a valid record
};

const char* to_string(DecodeError e);

struct Decoded {
    LogRecord record;
    std::size_t size = 0;   // bytes consumed, header included
};

// Appends one encoded record to `out`.
void encode(const LogRecord& record, std::vector<std::byte>& out);

// Decodes the record at the start of `in`. Never throws, never reads past `in`, and never
// allocates more than kMaxRecordPayload.
tl::expected<Decoded, DecodeError> decode(std::span<const std::byte> in);

// The result of scanning a whole log file.
struct ScanResult {
    std::vector<LogRecord> records;
    std::size_t valid_bytes = 0;   // everything after this is a torn tail, safe to cut off
    bool torn_tail = false;
};

// Scans a log file's bytes from the start. A torn tail (an incomplete last record, or
// zeros after the last record) is tolerated: it was never synced, so never acknowledged.
// Anything else wrong is corruption of data that may have been acknowledged, and is an
// error: the caller must refuse to start rather than serve a shortened log.
tl::expected<ScanResult, DecodeError> scan(std::span<const std::byte> file);

}  // namespace raftkv::store
