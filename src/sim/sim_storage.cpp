#include "sim/sim_storage.hpp"

#include <algorithm>

namespace raftkv::sim {

void SimStorage::save_hard_state(raft::Term current_term, std::optional<raft::NodeId> voted_for) {
    synced_.current_term = current_term;
    synced_.voted_for = voted_for;
}

void SimStorage::append(std::span<const raft::LogEntry> entries) {
    if (entries.empty()) return;
    pending_.emplace_back(Append{{entries.begin(), entries.end()}});
}

void SimStorage::truncate_suffix(raft::Index from) { pending_.emplace_back(Truncate{from}); }

void SimStorage::sync() {
    for (const Op& op : pending_) apply(synced_, op);
    pending_.clear();
    ++sync_count_;
}

raft::PersistentState SimStorage::load() const {
    raft::PersistentState view = synced_;
    for (const Op& op : pending_) apply(view, op);
    return view;
}

void SimStorage::save_snapshot(const raft::Snapshot& snapshot) {
    // Durable at once, like the hard state. Unsynced appends stay pending; whatever part of
    // them the snapshot covers is skipped when they are applied.
    synced_.snapshot = snapshot;
    std::erase_if(synced_.log, [&](const raft::LogEntry& e) { return e.index <= snapshot.last_included_index; });
}

std::size_t SimStorage::crash(CrashMode mode, Rng& rng) {
    std::size_t keep = 0;
    if (mode == CrashMode::KeepRandomPrefix) keep = rng.between(0, pending_.size());
    for (std::size_t i = 0; i < keep; ++i) apply(synced_, pending_[i]);
    const std::size_t lost = pending_.size() - keep;
    pending_.clear();
    return lost;
}

void SimStorage::apply(raft::PersistentState& s, const Op& op) {
    if (const auto* a = std::get_if<Append>(&op)) {
        const raft::Index covered = s.snapshot ? s.snapshot->last_included_index : 0;
        for (const raft::LogEntry& e : a->entries) {
            if (e.index > covered) s.log.push_back(e);
        }
    } else {
        const raft::Index from = std::get<Truncate>(op).from;
        std::erase_if(s.log, [from](const raft::LogEntry& e) { return e.index >= from; });
    }
}

}  // namespace raftkv::sim
