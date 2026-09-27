// Phase 6: histories for the linearizability checker.
//
// Each test runs a contended workload (4 clients, 3 keys, get/put/append) against a 5-node
// cluster under random faults and records every operation. With RAFTKV_HISTORY_DIR set, the
// histories are written out, and tests/lincheck/run.sh checks them with Porcupine
// (tools/lincheck). Without it, the tests still run the workload with every Raft invariant
// checked after every event.

#include <gtest/gtest.h>

#include <array>
#include <functional>
#include <map>

#include "history.hpp"
#include "raft_cluster.hpp"
#include "test_seeds.hpp"

namespace raftkv {
namespace {

using namespace std::chrono_literals;
using raft::NodeId;
using test::ClusterOptions;
using test::History;
using test::RaftCluster;
using test::seeds;

constexpr std::array<const char*, 3> kKeys{"x", "y", "z"};

// The workload's state. `next` issues a client's next operation; callbacks refer to it
// through a weak_ptr so that it does not own itself (a leak LeakSanitizer would report).
struct Workload {
    std::shared_ptr<History> history = std::make_shared<History>();
    std::shared_ptr<std::function<void(NodeId)>> next = std::make_shared<std::function<void(NodeId)>>();
};

// Starts the workload: every client runs operations back to back until `stop_issuing`.
// The history fills in as the simulation runs. Keep the Workload alive for the whole run.
Workload run_workload(RaftCluster& c, std::uint64_t seed, raft::Time stop_issuing) {
    Workload w;
    auto history = w.history;
    auto rng = std::make_shared<sim::Rng>(seed * 7919 + 17);
    auto counter = std::make_shared<std::map<NodeId, int>>();
    std::weak_ptr<std::function<void(NodeId)>> next = w.next;
    *w.next = [&c, history, rng, counter, next, stop_issuing](NodeId id) {
        if (c.sim().now() >= stop_issuing) return;
        const int n = (*counter)[id]++;
        const std::string key = kKeys[rng->between(0, kKeys.size() - 1)];
        const auto roll = rng->between(0, 9);
        const kv::Op op = roll < 4 ? kv::Op::Get : roll < 7 ? kv::Op::Put : kv::Op::Append;
        // Unique values, so the checker can tell every write apart.
        const std::string value = op == kv::Op::Get ? "" : std::format("{}.{};", id, n);
        const std::size_t i = history->invoke(id, op, key, value, c.sim().now());
        auto done = [&c, history, next, i, id](const kv::Result& r) {
            history->complete(i, r, c.sim().now());
            if (auto issue = next.lock()) (*issue)(id);
        };
        auto& client = c.client(id);
        if (op == kv::Op::Get) client.get(key, done);
        else if (op == kv::Op::Put) client.put(key, value, done);
        else client.append(key, value, done);
    };
    for (NodeId id : c.client_ids()) (*w.next)(id);
    return w;
}

// Faults for 10 s, then everything heals and every client's last operation completes, so
// the whole history has returned.
TEST(Linearizability, HealedRunsProduceCompleteHistories) {
    for (std::uint64_t seed : test::chaos_seeds(100)) {
        RaftCluster c(seed, 5, ClusterOptions{.kv = true, .clients = 4});
        c.quiet();
        c.schedule_chaos(seed, 10s);
        c.sim().start();
        const Workload w = run_workload(c, seed, raft::Time{10s});
        const auto& history = w.history;
        c.sim().run_until(raft::Time{10s});
        ASSERT_TRUE(c.sim().run_until([&] {
            for (NodeId id : c.client_ids()) if (c.client(id).busy()) return false;
            return true;
        }, raft::Time{30s})) << c.context();
        ASSERT_TRUE(c.violations().empty()) << testing::PrintToString(c.violations()) << c.context();
        EXPECT_GT(history->size(), 100u);
        EXPECT_EQ(history->pending(), 0u);
        history->write_if_requested(seed, "healed");
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

}  // namespace
}  // namespace raftkv
