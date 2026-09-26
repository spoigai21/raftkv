#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include <tl/expected.hpp>

#include "raft/types.hpp"

// Length-prefixed framing for raft::Message over a byte stream:
//
//   [len u32 little-endian][Envelope protobuf: from, to, method, payload]
//
// The length is checked against kMaxFrame before anything is allocated, so a peer (or a
// stray port scanner) cannot make us allocate gigabytes by sending four bytes.
namespace raftkv::net {

inline constexpr std::uint32_t kMaxFrame = 16u << 20;   // implementation guide §3.5

// One complete frame, ready to write.
std::string encode_frame(const raft::Message& m);

// Accumulates bytes from a stream and yields whole messages. A protocol error (oversized or
// undecodable frame) is sticky: the connection must be closed, since framing is lost.
class FrameReader {
public:
    void feed(std::span<const std::byte> bytes);

    // The next complete message, nullopt if more bytes are needed, or an error.
    tl::expected<std::optional<raft::Message>, std::string> next();

    std::size_t buffered() const { return buf_.size() - pos_; }

private:
    std::vector<std::byte> buf_;
    std::size_t pos_ = 0;   // start of the first unconsumed byte
    std::optional<std::string> error_;
};

}  // namespace raftkv::net
