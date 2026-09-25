#include "sim/sim_storage.hpp"

#include <gtest/gtest.h>

#include <set>
#include <vector>

namespace raftkv {
namespace {

using raft::LogEntry;
using sim::SimStorage;

std::vector<LogEntry> entries(raft::Index from, raft::Index to, raft::Term term) {
    std::vector<LogEntry> out;
    for (raft::Index i = from; i <= to; ++i) out.push_back({term, i, "cmd" + std::to_string(i)});
    return out;
}

TEST(SimStorage, LoadSeesUnsyncedWritesButSyncedDoesNot) {
    SimStorage s;
    s.append(entries(1, 3, 1));
    EXPECT_EQ(s.load().log.size(), 3u);
    EXPECT_TRUE(s.synced().log.empty());
    s.sync();
    EXPECT_EQ(s.synced().log, entries(1, 3, 1));
    EXPECT_EQ(s.unsynced_ops(), 0u);
}

TEST(SimStorage, TruncateSuffixRemovesFromIndexOnward) {
    SimStorage s;
    s.append(entries(1, 5, 1));
    s.truncate_suffix(3);
    s.append(entries(3, 4, 2));
    s.sync();
    std::vector<LogEntry> want = entries(1, 2, 1);
    for (auto& e : entries(3, 4, 2)) want.push_back(e);
    EXPECT_EQ(s.load().log, want);
}

TEST(SimStorage, HardStateIsDurableImmediately) {
    SimStorage s;
    sim::Rng rng(1);
    s.save_hard_state(7, 3);
    s.crash(SimStorage::CrashMode::DropUnsynced, rng);
    EXPECT_EQ(s.load().current_term, 7u);
    EXPECT_EQ(s.load().voted_for, 3u);
}

TEST(SimStorage, CrashDropsExactlyTheUnsyncedWrites) {
    SimStorage s;
    sim::Rng rng(1);
    s.append(entries(1, 2, 1));
    s.sync();
    s.append(entries(3, 4, 1));
    s.truncate_suffix(2);
    EXPECT_EQ(s.crash(SimStorage::CrashMode::DropUnsynced, rng), 2u);
    EXPECT_EQ(s.load().log, entries(1, 2, 1));
}

// A crash that keeps some unsynced writes may keep only a prefix of them, in order: never a
// later write without every earlier one.
TEST(SimStorage, KeepRandomPrefixKeepsAnOrderedPrefix) {
    // The four states reachable by keeping 0, 1, 2 or 3 of the unsynced operations below.
    std::vector<std::vector<LogEntry>> valid;
    valid.push_back(entries(1, 2, 1));
    valid.push_back(entries(1, 4, 1));
    valid.push_back(entries(1, 1, 1));
    valid.push_back(entries(1, 1, 1));
    for (auto& e : entries(2, 3, 2)) valid.back().push_back(e);

    std::set<std::size_t> seen;
    for (std::uint64_t seed = 1; seed <= 200; ++seed) {
        SimStorage s;
        sim::Rng rng(seed);
        s.append(entries(1, 2, 1));
        s.sync();
        s.append(entries(3, 4, 1));   // op 1
        s.truncate_suffix(2);         // op 2
        s.append(entries(2, 3, 2));   // op 3
        s.crash(SimStorage::CrashMode::KeepRandomPrefix, rng);
        const auto it = std::ranges::find(valid, s.load().log);
        ASSERT_NE(it, valid.end()) << "seed " << seed << ": not a prefix of the unsynced writes";
        seen.insert(static_cast<std::size_t>(it - valid.begin()));
        EXPECT_EQ(s.unsynced_ops(), 0u);
    }
    EXPECT_EQ(seen.size(), valid.size()) << "every prefix length should occur across 200 seeds";
}

}  // namespace
}  // namespace raftkv
