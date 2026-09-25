#pragma once

#include <optional>
#include <span>

#include "raft/types.hpp"

namespace raftkv::raft {

// Durable state behind an interface (implementation guide §3.4): SimStorage in the
// simulator, FileStorage (Phase 4) in a real node.
class Storage {
public:
    virtual ~Storage() = default;

    // Durable when it returns. Raft calls this before replying to the RPC that changed it.
    virtual void save_hard_state(Term current_term, std::optional<NodeId> voted_for) = 0;

    // Not durable until sync(). Entries must continue the log without gaps.
    virtual void append(std::span<const LogEntry> entries) = 0;

    // Deletes every entry with index >= from. Not durable until sync().
    virtual void truncate_suffix(Index from) = 0;

    // Makes every earlier append/truncate durable.
    virtual void sync() = 0;

    // The state as this process currently sees it, synced or not. A node calls this once
    // on startup, when only durable state is left.
    virtual PersistentState load() const = 0;
};

}  // namespace raftkv::raft
