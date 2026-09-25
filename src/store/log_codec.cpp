#include "store/log_codec.hpp"

#include <algorithm>
#include <array>

namespace raftkv::store {

namespace {

constexpr std::array<std::uint32_t, 256> make_crc_table() {
    std::array<std::uint32_t, 256> table{};
    for (std::uint32_t i = 0; i < 256; ++i) {
        std::uint32_t c = i;
        for (int k = 0; k < 8; ++k) c = (c & 1) ? 0xedb88320u ^ (c >> 1) : c >> 1;
        table[i] = c;
    }
    return table;
}

constexpr auto kCrcTable = make_crc_table();

constexpr std::uint8_t kEntry = 0x01;
constexpr std::uint8_t kTruncateFrom = 0x02;

void put_u32(std::vector<std::byte>& out, std::uint32_t v) {
    for (int i = 0; i < 4; ++i) out.push_back(static_cast<std::byte>(v >> (8 * i)));
}

void put_u64(std::vector<std::byte>& out, std::uint64_t v) {
    for (int i = 0; i < 8; ++i) out.push_back(static_cast<std::byte>(v >> (8 * i)));
}

std::uint32_t get_u32(std::span<const std::byte> in) {
    std::uint32_t v = 0;
    for (int i = 0; i < 4; ++i) v |= std::to_integer<std::uint32_t>(in[static_cast<std::size_t>(i)]) << (8 * i);
    return v;
}

std::uint64_t get_u64(std::span<const std::byte> in) {
    std::uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v |= std::to_integer<std::uint64_t>(in[static_cast<std::size_t>(i)]) << (8 * i);
    return v;
}

tl::expected<LogRecord, DecodeError> decode_payload(std::span<const std::byte> p) {
    if (p.empty()) return tl::unexpected(DecodeError::BadPayload);
    const auto type = std::to_integer<std::uint8_t>(p[0]);
    p = p.subspan(1);
    if (type == kEntry && p.size() >= 16) {
        raft::LogEntry e;
        e.term = get_u64(p);
        e.index = get_u64(p.subspan(8));
        const auto cmd = p.subspan(16);
        e.command.assign(reinterpret_cast<const char*>(cmd.data()), cmd.size());
        return e;
    }
    if (type == kTruncateFrom && p.size() == 8) return TruncateFrom{get_u64(p)};
    return tl::unexpected(DecodeError::BadPayload);
}

bool all_zero(std::span<const std::byte> in) {
    return std::ranges::all_of(in, [](std::byte b) { return b == std::byte{0}; });
}

}  // namespace

std::uint32_t crc32(std::span<const std::byte> data, std::uint32_t crc) {
    crc = ~crc;
    for (std::byte b : data) crc = kCrcTable[(crc ^ std::to_integer<std::uint32_t>(b)) & 0xffu] ^ (crc >> 8);
    return ~crc;
}

const char* to_string(DecodeError e) {
    switch (e) {
        case DecodeError::Truncated: return "truncated record";
        case DecodeError::BadHeader: return "record header checksum mismatch";
        case DecodeError::TooLarge: return "record larger than the maximum";
        case DecodeError::BadChecksum: return "record payload checksum mismatch";
        case DecodeError::BadPayload: return "record payload is not a valid record";
    }
    return "?";
}

void encode(const LogRecord& record, std::vector<std::byte>& out) {
    std::vector<std::byte> payload;
    if (const auto* e = std::get_if<raft::LogEntry>(&record)) {
        payload.reserve(17 + e->command.size());
        payload.push_back(std::byte{kEntry});
        put_u64(payload, e->term);
        put_u64(payload, e->index);
        const auto cmd = std::as_bytes(std::span(e->command));
        payload.insert(payload.end(), cmd.begin(), cmd.end());
    } else {
        payload.push_back(std::byte{kTruncateFrom});
        put_u64(payload, std::get<TruncateFrom>(record).index);
    }

    std::vector<std::byte> header;
    put_u32(header, static_cast<std::uint32_t>(payload.size()));
    put_u32(header, crc32(payload));
    put_u32(header, crc32(std::span(header).first(8)));
    out.insert(out.end(), header.begin(), header.end());
    out.insert(out.end(), payload.begin(), payload.end());
}

tl::expected<Decoded, DecodeError> decode(std::span<const std::byte> in) {
    if (in.size() < kRecordHeaderSize) return tl::unexpected(DecodeError::Truncated);
    if (crc32(in.first(8)) != get_u32(in.subspan(8))) return tl::unexpected(DecodeError::BadHeader);
    const std::uint32_t len = get_u32(in);
    if (len > kMaxRecordPayload) return tl::unexpected(DecodeError::TooLarge);
    if (in.size() - kRecordHeaderSize < len) return tl::unexpected(DecodeError::Truncated);

    const auto payload = in.subspan(kRecordHeaderSize, len);
    if (crc32(payload) != get_u32(in.subspan(4))) return tl::unexpected(DecodeError::BadChecksum);
    return decode_payload(payload).map([&](LogRecord r) {
        return Decoded{.record = std::move(r), .size = kRecordHeaderSize + len};
    });
}

tl::expected<ScanResult, DecodeError> scan(std::span<const std::byte> file) {
    ScanResult result;
    std::size_t pos = 0;
    while (pos < file.size()) {
        const auto rest = file.subspan(pos);
        auto d = decode(rest);
        if (d) {
            result.records.push_back(std::move(d->record));
            pos += d->size;
            continue;
        }
        // Only two things can follow the last synced record: part of a record whose write
        // never finished, or zeros where the file grew but the data never landed.
        if (d.error() == DecodeError::Truncated || all_zero(rest)) {
            result.torn_tail = true;
            break;
        }
        return tl::unexpected(d.error());
    }
    result.valid_bytes = pos;
    return result;
}

}  // namespace raftkv::store
