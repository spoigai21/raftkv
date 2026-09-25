#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include <tl/expected.hpp>

#include "raft/types.hpp"

// The key-value layer's commands and client RPCs as plain structs. Protobuf is only the
// encoding, confined to kv/messages.cpp.
namespace raftkv::kv {

enum class Op { Get, Put, Append };

// One client operation. It is what gets written to the Raft log, and (client_id, seq) is
// what duplicate detection keys on.
struct Command {
    std::uint64_t client_id = 0;
    std::uint64_t seq = 0;
    Op op = Op::Get;
    std::string key;
    std::string value;   // Put/Append only

    friend bool operator==(const Command&, const Command&) = default;
};

struct Result {
    std::string value;   // Get: the value; Put/Append: empty
    bool found = false;  // Get: whether the key existed

    friend bool operator==(const Result&, const Result&) = default;
};

enum class Status { Ok, NotLeader };

struct KvReply {
    std::uint64_t client_id = 0;
    std::uint64_t seq = 0;
    Status status = Status::Ok;
    Result result;
    std::optional<raft::NodeId> leader_hint;   // NotLeader only
};

inline constexpr std::string_view kKvRequest = "KvRequest";
inline constexpr std::string_view kKvReply = "KvReply";

// Log payloads. A Command always encodes to a non-empty string (client_id and seq are
// non-zero), so it can never be confused with a leader's empty no-op entry.
std::string encode_command(const Command& c);
tl::expected<Command, std::string> decode_command(const std::string& bytes);

raft::Message encode_request(raft::NodeId to, const Command& c);
tl::expected<Command, std::string> parse_request(const raft::Message& m);

raft::Message encode_reply(raft::NodeId to, const KvReply& r);
tl::expected<KvReply, std::string> parse_reply(const raft::Message& m);

bool is_kv_message(const raft::Message& m);
std::string describe(const raft::Message& m);   // for the simulator's event log
const char* to_string(Op op);

}  // namespace raftkv::kv
