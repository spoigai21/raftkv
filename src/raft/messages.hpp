#pragma once

#include <string>
#include <variant>
#include <vector>

#include <tl/expected.hpp>

#include "raft/types.hpp"

// Raft's RPCs (Figure 2 of the paper) as plain structs. Protobuf is only the wire format:
// encode()/parse() are the one place that touches the generated code.
namespace raftkv::raft {

struct RequestVote {
    Term term = 0;
    NodeId candidate_id = 0;
    Index last_log_index = 0;
    Term last_log_term = 0;
};

struct RequestVoteReply {
    Term term = 0;
    bool vote_granted = false;
};

struct AppendEntries {
    Term term = 0;
    NodeId leader_id = 0;
    Index prev_log_index = 0;
    Term prev_log_term = 0;
    std::vector<LogEntry> entries;   // empty: a heartbeat
    Index leader_commit = 0;
};

struct AppendEntriesReply {
    Term term = 0;
    bool success = false;
    // On success, the follower's log matches the leader's up to here. Sent explicitly
    // because replies can arrive out of order.
    Index match_index = 0;
    // On failure, a hint so the leader can skip back a whole term per round trip instead of
    // one entry: the follower's conflicting term (0 if its log is just too short) and the
    // first index it holds for that term (or its last index + 1).
    Index conflict_index = 0;
    Term conflict_term = 0;
};

using Rpc = std::variant<RequestVote, RequestVoteReply, AppendEntries, AppendEntriesReply>;

// Wraps an RPC in a Message addressed to `to`. The sender is filled in by the Env.
Message encode(NodeId to, const Rpc& rpc);

// Decodes a received Message. Malformed or unknown messages are errors, not exceptions.
tl::expected<Rpc, std::string> parse(const Message& m);

// One-line summary for the event log, e.g. "RequestVote term=4 last=3/2".
std::string describe(const Message& m);

}  // namespace raftkv::raft
