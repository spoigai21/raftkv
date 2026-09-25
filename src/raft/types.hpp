#pragma once

#include <chrono>
#include <compare>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

// Index and ID conventions fixed in raftkv-implementation.md §3.3.
namespace raftkv::raft {

using Term = std::uint64_t;
using Index = std::uint64_t;   // the log is 1-indexed; index 0 is a term-0 sentinel
using NodeId = std::uint32_t;

// Time is whatever the Env says it is: virtual in the simulator, monotonic in a real node.
// It is a plain value type on purpose, so nothing can ask the OS for the current time.
using Duration = std::chrono::microseconds;

struct Time {
    Duration since_start{0};

    friend constexpr auto operator<=>(Time, Time) = default;
    friend constexpr Time operator+(Time t, Duration d) { return Time{t.since_start + d}; }
    friend constexpr Duration operator-(Time a, Time b) { return a.since_start - b.since_start; }
};

using TimerId = std::uint64_t;
using TimerTag = std::uint64_t;   // opaque to the Env; the node decides what it means

struct Message {
    NodeId from = 0;
    NodeId to = 0;
    std::string method;
    std::string payload;
};

struct LogEntry {
    Term term = 0;
    Index index = 0;
    std::string command;

    friend bool operator==(const LogEntry&, const LogEntry&) = default;
};

// Everything a node persists: what a restarted node reads back from Storage.
struct PersistentState {
    Term current_term = 0;
    std::optional<NodeId> voted_for;
    std::vector<LogEntry> log;   // entries 1..N in order; no sentinel

    friend bool operator==(const PersistentState&, const PersistentState&) = default;
};

}  // namespace raftkv::raft
