#include "net/frame.hpp"

#include <format>

#include "net/net.pb.h"

namespace raftkv::net {

std::string encode_frame(const raft::Message& m) {
    pb::Envelope env;
    env.set_from(m.from);
    env.set_to(m.to);
    env.set_method(m.method);
    env.set_payload(m.payload);
    const std::string body = env.SerializeAsString();
    std::string frame;
    frame.reserve(4 + body.size());
    const auto len = static_cast<std::uint32_t>(body.size());
    for (int i = 0; i < 4; ++i) frame.push_back(static_cast<char>((len >> (8 * i)) & 0xff));
    frame += body;
    return frame;
}

void FrameReader::feed(std::span<const std::byte> bytes) {
    // Compact once the consumed prefix dominates, so the buffer does not grow forever.
    if (pos_ > 0 && pos_ >= buf_.size() / 2) {
        buf_.erase(buf_.begin(), buf_.begin() + static_cast<std::ptrdiff_t>(pos_));
        pos_ = 0;
    }
    buf_.insert(buf_.end(), bytes.begin(), bytes.end());
}

tl::expected<std::optional<raft::Message>, std::string> FrameReader::next() {
    if (error_) return tl::unexpected(*error_);
    if (buffered() < 4) return std::nullopt;

    std::uint32_t len = 0;
    for (int i = 0; i < 4; ++i) {
        len |= std::to_integer<std::uint32_t>(buf_[pos_ + static_cast<std::size_t>(i)]) << (8 * i);
    }
    if (len > kMaxFrame) {
        error_ = std::format("frame of {} bytes exceeds the {} byte limit", len, kMaxFrame);
        return tl::unexpected(*error_);
    }
    if (buffered() - 4 < len) return std::nullopt;

    pb::Envelope env;
    if (!env.ParseFromArray(buf_.data() + pos_ + 4, static_cast<int>(len))) {
        error_ = "undecodable frame";
        return tl::unexpected(*error_);
    }
    pos_ += 4 + len;
    return raft::Message{.from = env.from(), .to = env.to(), .method = env.method(),
                         .payload = env.payload()};
}

}  // namespace raftkv::net
