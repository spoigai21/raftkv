#include "store/log_codec.hpp"

#include <gtest/gtest.h>

#include <string_view>

#include "sim/rng.hpp"

namespace raftkv {
namespace {

using raft::LogEntry;
using store::DecodeError;
using store::LogRecord;
using store::TruncateFrom;

std::vector<std::byte> bytes_of(std::string_view s) {
    const auto b = std::as_bytes(std::span(s));
    return {b.begin(), b.end()};
}

std::vector<std::byte> encode_all(const std::vector<LogRecord>& records) {
    std::vector<std::byte> out;
    for (const auto& r : records) store::encode(r, out);
    return out;
}

// Entries with commands of different lengths (including empty and binary), and a truncation.
std::vector<LogRecord> sample() {
    return {LogEntry{1, 1, "put a 1"}, LogEntry{1, 2, ""}, LogEntry{2, 3, std::string("\0\xff\n", 3)},
            TruncateFrom{3}, LogEntry{3, 3, std::string(300, 'x')}};
}

TEST(Crc32, MatchesTheStandardCheckValue) {
    EXPECT_EQ(store::crc32(bytes_of("123456789")), 0xcbf43926u);
    EXPECT_EQ(store::crc32(bytes_of("")), 0u);
}

TEST(Crc32, CanBeComputedIncrementally) {
    const auto all = bytes_of("hello, raft");
    const auto head = std::span(all).first(5);
    const auto tail = std::span(all).subspan(5);
    EXPECT_EQ(store::crc32(tail, store::crc32(head)), store::crc32(all));
}

TEST(LogCodec, RoundTripsEveryRecordKind) {
    const auto records = sample();
    const auto bytes = encode_all(records);
    auto scanned = store::scan(bytes);
    ASSERT_TRUE(scanned.has_value()) << store::to_string(scanned.error());
    EXPECT_EQ(scanned->records, records);
    EXPECT_EQ(scanned->valid_bytes, bytes.size());
    EXPECT_FALSE(scanned->torn_tail);
}

TEST(LogCodec, EmptyFileIsAnEmptyLog) {
    auto scanned = store::scan({});
    ASSERT_TRUE(scanned.has_value());
    EXPECT_TRUE(scanned->records.empty());
}

TEST(LogCodec, OversizedLengthIsRejectedBeforeAllocating) {
    // A well-formed header that claims 4 GiB of payload.
    std::vector<std::byte> bytes;
    store::encode(LogEntry{1, 1, "x"}, bytes);
    const std::uint32_t huge = 0xffffffffu;
    for (int i = 0; i < 4; ++i) bytes[static_cast<std::size_t>(i)] = static_cast<std::byte>(huge >> (8 * i));
    const auto hcrc = store::crc32(std::span(bytes).first(8));
    for (int i = 0; i < 4; ++i) bytes[8 + static_cast<std::size_t>(i)] = static_cast<std::byte>(hcrc >> (8 * i));
    auto d = store::decode(bytes);
    ASSERT_FALSE(d.has_value());
    EXPECT_EQ(d.error(), DecodeError::TooLarge);
}

// A crash can cut the file anywhere. Every cut must recover exactly the records that were
// fully written before it, and report the rest as a torn tail.
TEST(LogCodec, CutAtEveryByteRecoversTheCompletePrefix) {
    const auto records = sample();
    std::vector<std::size_t> ends;   // byte offset where each record ends
    std::vector<std::byte> bytes;
    for (const auto& r : records) {
        store::encode(r, bytes);
        ends.push_back(bytes.size());
    }
    for (std::size_t cut = 0; cut <= bytes.size(); ++cut) {
        auto scanned = store::scan(std::span(bytes).first(cut));
        ASSERT_TRUE(scanned.has_value()) << "cut at " << cut;
        std::size_t complete = 0;
        while (complete < ends.size() && ends[complete] <= cut) ++complete;
        EXPECT_EQ(scanned->records.size(), complete) << "cut at " << cut;
        EXPECT_EQ(scanned->valid_bytes, complete == 0 ? 0 : ends[complete - 1]) << "cut at " << cut;
        EXPECT_EQ(scanned->torn_tail, scanned->valid_bytes != cut) << "cut at " << cut;
    }
}

// The file grew but the last write never landed: zeros after the last record.
TEST(LogCodec, ZeroFilledTailIsTorn) {
    auto bytes = encode_all(sample());
    const std::size_t valid = bytes.size();
    bytes.resize(valid + 4096, std::byte{0});
    auto scanned = store::scan(bytes);
    ASSERT_TRUE(scanned.has_value());
    EXPECT_TRUE(scanned->torn_tail);
    EXPECT_EQ(scanned->valid_bytes, valid);
    EXPECT_EQ(scanned->records, sample());
}

// Damage to a complete record is never mistaken for a torn tail: every single-bit flip,
// anywhere in the file, including the last record and its length field, is an error.
TEST(LogCodec, EveryBitFlipIsDetectedAsCorruption) {
    const auto clean = encode_all(sample());
    for (std::size_t i = 0; i < clean.size(); ++i) {
        for (int bit = 0; bit < 8; ++bit) {
            auto bytes = clean;
            bytes[i] ^= static_cast<std::byte>(1 << bit);
            auto scanned = store::scan(bytes);
            EXPECT_FALSE(scanned.has_value()) << "flip byte " << i << " bit " << bit
                                              << " went undetected";
        }
    }
}

TEST(LogCodec, GarbageAfterTheLastRecordIsCorruptionNotATornTail) {
    auto bytes = encode_all(sample());
    for (std::byte b : bytes_of("this is not zeros and not a record header")) bytes.push_back(b);
    auto scanned = store::scan(bytes);
    ASSERT_FALSE(scanned.has_value());
    EXPECT_EQ(scanned.error(), DecodeError::BadHeader);
}

TEST(LogCodec, ValidChecksumsButUnknownTypeIsBadPayload) {
    std::vector<std::byte> payload{std::byte{0x7f}};
    std::vector<std::byte> bytes;
    auto put = [&](std::uint32_t v) {
        for (int i = 0; i < 4; ++i) bytes.push_back(static_cast<std::byte>(v >> (8 * i)));
    };
    put(1);
    put(store::crc32(payload));
    put(store::crc32(std::span(bytes).first(8)));
    bytes.push_back(payload[0]);
    auto d = store::decode(bytes);
    ASSERT_FALSE(d.has_value());
    EXPECT_EQ(d.error(), DecodeError::BadPayload);
}

// A portable stand-in for the libFuzzer target: random and mutated inputs must never crash,
// hang or read out of bounds (ASan/UBSan watch that), and anything that decodes must
// re-encode to the same bytes.
TEST(LogCodec, RandomInputsNeverCrashAndDecodedRecordsRoundTrip) {
    sim::Rng rng(1234);
    const auto clean = encode_all(sample());
    for (int iter = 0; iter < 100'000; ++iter) {
        std::vector<std::byte> in;
        if (iter % 2 == 0) {
            in.resize(rng.between(0, 64));
            for (auto& b : in) b = static_cast<std::byte>(rng.next());
        } else {
            in = clean;
            const auto flips = rng.between(1, 4);
            for (std::uint64_t f = 0; f < flips; ++f) in[rng.between(0, in.size() - 1)] ^= static_cast<std::byte>(rng.next() | 1);
            in.resize(rng.between(0, in.size()));
        }
        if (auto d = store::decode(in)) {
            std::vector<std::byte> again;
            store::encode(d->record, again);
            ASSERT_EQ(again, std::vector<std::byte>(in.begin(), in.begin() + static_cast<std::ptrdiff_t>(d->size)));
        }
        (void)store::scan(in);
    }
}

}  // namespace
}  // namespace raftkv
