#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>

#include <tl/expected.hpp>

#include "kv/messages.hpp"

namespace raftkv::kv {

// The replicated key-value map. Deterministic: every replica that applies the same commands
// in the same order ends in the same state, dedup table included.
//
// Duplicate detection (implementation guide Phase 5): a client retries a timed-out request
// with the same (client_id, seq), so the same command can be in the log twice. The table
// remembers each client's last applied seq and its reply; a command at or below that seq is
// not applied again. It changes only here, when entries are applied, never when requests
// arrive, so it is identical on every replica. Clients have one request outstanding at a time,
// so one entry per client is enough. Sessions are never evicted (a v1 limit).
class StateMachine {
public:
    // Applies `c` unless it is a duplicate, and returns its reply either way.
    Result apply(const Command& c);

    std::optional<std::string> get(const std::string& key) const;

    // The reply to (client_id, seq) if that request is the client's last applied one, so a
    // resend can be answered without going through the log again.
    std::optional<Result> applied_reply(std::uint64_t client_id, std::uint64_t seq) const;

    // For Raft snapshots: the whole state, dedup table included (implementation guide
    // Phase 8), and back. Equal states encode to equal bytes.
    std::string serialize() const;
    static tl::expected<StateMachine, std::string> deserialize(const std::string& bytes);

    // Same data and same dedup table (the duplicates counter is a statistic, not state).
    friend bool operator==(const StateMachine& a, const StateMachine& b) {
        return a.data_ == b.data_ && a.sessions_ == b.sessions_;
    }
    std::size_t size() const { return data_.size(); }
    std::uint64_t duplicates_suppressed() const { return duplicates_; }

private:
    struct Session {
        std::uint64_t last_seq = 0;
        Result last_result;
        friend bool operator==(const Session&, const Session&) = default;
    };

    std::map<std::string, std::string> data_;
    std::map<std::uint64_t, Session> sessions_;
    std::uint64_t duplicates_ = 0;
};

}  // namespace raftkv::kv
