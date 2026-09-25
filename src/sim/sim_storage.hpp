#pragma once

#include <cstddef>
#include <variant>
#include <vector>

#include "raft/storage.hpp"
#include "sim/rng.hpp"

namespace raftkv::sim {

// In-memory Storage that remembers which writes were synced, so a simulated crash can lose
// exactly the ones a real disk could lose (implementation guide §3.4).
class SimStorage final : public raft::Storage {
public:
    enum class CrashMode {
        DropUnsynced,       // nothing unsynced reached the disk
        KeepRandomPrefix,   // the OS flushed some unsynced writes, in order, before the crash
    };

    void save_hard_state(raft::Term current_term, std::optional<raft::NodeId> voted_for) override;
    void append(std::span<const raft::LogEntry> entries) override;
    void truncate_suffix(raft::Index from) override;
    void sync() override;
    raft::PersistentState load() const override;

    // Simulator-only. Returns how many unsynced operations were lost.
    std::size_t crash(CrashMode mode, Rng& rng);

    const raft::PersistentState& synced() const { return synced_; }
    std::size_t unsynced_ops() const { return pending_.size(); }
    std::size_t sync_count() const { return sync_count_; }

private:
    struct Append { std::vector<raft::LogEntry> entries; };
    struct Truncate { raft::Index from; };
    using Op = std::variant<Append, Truncate>;

    static void apply(raft::PersistentState& s, const Op& op);

    raft::PersistentState synced_;
    std::vector<Op> pending_;
    std::size_t sync_count_ = 0;
};

}  // namespace raftkv::sim
