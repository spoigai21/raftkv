// Phase 6: histories for the linearizability checker.
//
// Each test runs a contended workload (4 clients, 3 keys, get/put/append) against a 5-node
// cluster under random faults and records every operation. With RAFTKV_HISTORY_DIR set, the
// histories are written out, and tests/lincheck/run.sh checks them with Porcupine
// (tools/lincheck). Without it, the tests still run the workload with every Raft invariant
// checked after every event.

#include <gtest/gtest.h>

#include "history.hpp"
#include "raft_cluster.hpp"
#include "test_seeds.hpp"
#include "workload.hpp"

namespace raftkv {
namespace {

using namespace std::chrono_literals;
using raft::NodeId;
using test::ClusterOptions;
using test::History;
using test::RaftCluster;
using test::seeds;
using test::Workload;
using test::run_workload;

// Faults for 10 s, then everything heals and every client's last operation completes, so
// the whole history has returned.
TEST(Linearizability, HealedRunsProduceCompleteHistories) {
    std::size_t total = 0;
    const auto runs = test::chaos_seed_count(100);
    for (std::uint64_t seed : seeds(runs)) {
        RaftCluster c(seed, 5, ClusterOptions{.kv = true, .clients = 4});
        c.quiet();
        c.schedule_chaos(seed, 10s);
        c.sim().start();
        const Workload w = run_workload(c, seed, raft::Time{10s});
        const auto& history = w.history;
        c.sim().run_until(raft::Time{10s});
        ASSERT_TRUE(test::wait_for_clients_idle(c, raft::Time{30s})) << c.context();
        ASSERT_TRUE(c.violations().empty()) << testing::PrintToString(c.violations()) << c.context();
        // Heavy chaos can keep a run to a few dozen operations; the total is what must be large.
        EXPECT_GT(history->size(), 0u);
        total += history->size();
        EXPECT_EQ(history->pending(), 0u);
        history->write_if_requested(seed, "healed");
    }
    if (std::getenv("RAFTKV_SEED") == nullptr) {
        EXPECT_GT(total, 200u * runs) << "the workload made little progress across all seeds";
    }
}

// The run stops in the middle of the faults. Operations whose clients are still retrying
// never return; the checker must accept them either having happened or not.
TEST(Linearizability, CutOffRunsLeaveOperationsUnresolved) {
    std::size_t pending = 0;
    std::size_t total = 0;
    const auto runs = test::chaos_seed_count(100);
    for (std::uint64_t seed : seeds(runs)) {
        RaftCluster c(seed, 5, ClusterOptions{.kv = true, .clients = 4});
        c.quiet();
        c.schedule_chaos(seed, 20s);   // still going when the run stops at 8 s
        c.sim().start();
        const Workload w = run_workload(c, seed, raft::Time{8s});
        const auto& history = w.history;
        c.sim().run_until(raft::Time{8s});
        ASSERT_TRUE(c.violations().empty()) << testing::PrintToString(c.violations()) << c.context();
        // Some seeds keep the cluster without a majority for most of the run, so only a
        // few operations complete; that is a valid history too.
        EXPECT_GT(history->size(), 0u);
        total += history->size();
        pending += history->pending();
        history->write_if_requested(seed, "cutoff");
    }
    if (std::getenv("RAFTKV_SEED") == nullptr) {
        EXPECT_GT(total, 100u * runs) << "the workload made little progress across all seeds";
        EXPECT_GT(pending, 0u) << "no run ended with an unresolved operation; that path went untested";
    }
}

// Phase 8: the same workload and faults with snapshots every 25 entries, so lagging nodes
// catch up by InstallSnapshot and restarted ones restore from a snapshot. A snapshot that
// lost a write, or forgot the dedup table, shows up here as a non-linearizable history.
TEST(Linearizability, HealedRunsWithFrequentSnapshots) {
    for (std::uint64_t seed : test::chaos_seeds(100)) {
        RaftCluster c(seed, 5, ClusterOptions{.kv = true, .clients = 4, .snapshot_every = 25});
        c.quiet();
        c.schedule_chaos(seed, 10s);
        c.sim().start();
        const Workload w = run_workload(c, seed, raft::Time{10s});
        c.sim().run_until(raft::Time{10s});
        ASSERT_TRUE(test::wait_for_clients_idle(c, raft::Time{30s})) << c.context();
        ASSERT_TRUE(c.violations().empty()) << testing::PrintToString(c.violations()) << c.context();
        EXPECT_GT(c.raft(1) ? c.raft(1)->snapshot_index() : 1u, 0u) << "no snapshot was ever taken";
        EXPECT_EQ(w.history->pending(), 0u);
        w.history->write_if_requested(seed, "snapshots");
    }
}

}  // namespace
}  // namespace raftkv
