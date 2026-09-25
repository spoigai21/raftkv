#include <gtest/gtest.h>

#include <map>

#include "raft_cluster.hpp"
#include "test_seeds.hpp"

namespace raftkv {
namespace {

using namespace std::chrono_literals;
using kv::Client;
using kv::Result;
using raft::NodeId;
using test::ClusterOptions;
using test::RaftCluster;
using test::seeds;

#define EXPECT_SAFE(c) EXPECT_TRUE((c).violations().empty()) \
    << testing::PrintToString((c).violations()) << "\n" << (c).context()

// Starts an operation and runs the simulation until it completes (or `timeout` passes).
template <class Start>
std::optional<Result> await(RaftCluster& c, NodeId client, Start start, raft::Duration timeout = 5s) {
    std::optional<Result> out;
    start(c.client(client), [&out](const Result& r) { out = r; });
    c.sim().run_until([&] { return out.has_value(); }, c.sim().now() + timeout);
    return out;
}

std::optional<Result> put(RaftCluster& c, NodeId cl, std::string k, std::string v) {
    return await(c, cl, [&](Client& x, auto done) { x.put(k, v, done); });
}
std::optional<Result> append(RaftCluster& c, NodeId cl, std::string k, std::string v) {
    return await(c, cl, [&](Client& x, auto done) { x.append(k, v, done); });
}
std::optional<Result> get(RaftCluster& c, NodeId cl, std::string k) {
    return await(c, cl, [&](Client& x, auto done) { x.get(k, done); });
}

TEST(Kv, PutGetAppendThroughTheCluster) {
    RaftCluster c(1, 3, ClusterOptions{.kv = true, .clients = 1});
    c.sim().start();
    ASSERT_TRUE(put(c, 101, "k", "a"));
    ASSERT_TRUE(append(c, 101, "k", "b"));
    auto r = get(c, 101, "k");
    ASSERT_TRUE(r) << c.context();
    EXPECT_EQ(*r, (Result{.value = "ab", .found = true}));
    auto missing = get(c, 101, "nope");
    ASSERT_TRUE(missing);
    EXPECT_FALSE(missing->found);
    EXPECT_SAFE(c);
}

TEST(Kv, ClientFindsTheLeaderFromAnyServer) {
    int redirected = 0;
    for (std::uint64_t seed : seeds(20)) {
        RaftCluster c(seed, 5, ClusterOptions{.kv = true, .clients = 1});
        c.sim().start();
        ASSERT_TRUE(c.wait_for_leader(2s));
        ASSERT_TRUE(put(c, 101, "k", "v")) << c.context();
        redirected += c.client(101).stats().not_leader > 0;
    }
    EXPECT_GT(redirected, 0) << "no seed ever started at a follower; the redirect path went untested";
}

// The Phase 5 done-gate. Ten times over: the client's Append commits, and the leader is
// killed in the instant between commit and apply, so no reply is ever sent. The client times
// out and retries the same (client_id, seq) on the new leader, where the entry is already
// committed. Without duplicate detection the retry would append a second time.
TEST(Kv, AppendSurvivesLeaderFailoverWithoutDuplication) {
    for (std::uint64_t seed : seeds(20)) {
        RaftCluster c(seed, 3, ClusterOptions{.kv = true, .clients = 1});
        c.sim().start();
        ASSERT_TRUE(c.wait_for_leader(2s));
        std::string want;
        for (int i = 0; i < 10; ++i) {
            const std::string piece = std::to_string(i) + ",";
            std::optional<Result> done;
            c.client(101).append("k", piece, [&](const Result& r) { done = r; });

            // Find the leader holding the entry, and kill it as soon as the entry commits.
            const auto committed_somewhere = [&]() -> std::optional<NodeId> {
                for (NodeId id : c.ids()) {
                    auto* r = c.raft(id);
                    if (r && r->role() == raft::Role::Leader && r->commit_index() > r->last_applied()) {
                        return id;
                    }
                }
                return std::nullopt;
            };
            ASSERT_TRUE(c.sim().run_until([&] { return committed_somewhere().has_value(); },
                                          c.sim().now() + 5s)) << c.context();
            const NodeId victim = *committed_somewhere();
            c.sim().crash(victim);
            c.sim().schedule(c.sim().now() + 300ms, "restart", [&c, victim] { c.sim().restart(victim); });

            ASSERT_TRUE(c.sim().run_until([&] { return done.has_value(); }, c.sim().now() + 5s))
                << "append " << i << " never completed\n" << c.context();
            want += piece;
        }
        c.sim().run_for(1s);
        auto r = get(c, 101, "k");
        ASSERT_TRUE(r) << c.context();
        EXPECT_EQ(r->value, want) << "seed " << seed;
        EXPECT_GE(c.client(101).stats().timeouts, 10u) << "every reply was lost, so every op timed out";

        std::uint64_t suppressed = 0;
        for (NodeId id : c.ids()) {
            if (auto* s = c.server(id)) suppressed += s->state().duplicates_suppressed();
        }
        EXPECT_GT(suppressed, 0u) << "the retry never reached the log, so dedup went untested";
        EXPECT_SAFE(c);
    }
}

// A leader that is deposed while a request is pending answers NotLeader at once, instead of
// leaving the client to time out.
TEST(Kv, DeposedLeaderFailsPendingRequestsPromptly) {
    for (std::uint64_t seed : seeds(20)) {
        RaftCluster c(seed, 3, ClusterOptions{.kv = true, .clients = 1});
        c.sim().start();
        ASSERT_TRUE(c.wait_for_leader(2s));
        ASSERT_TRUE(put(c, 101, "warmup", "x"));   // the client now knows the leader
        const NodeId leader = *c.leader();
        const raft::Index before = c.raft(leader)->last_log_index();

        std::optional<Result> done;
        c.client(101).put("k", "v", [&](const Result& r) { done = r; });
        ASSERT_TRUE(c.sim().run_until([&] { return c.raft(leader)->last_log_index() > before; },
                                      c.sim().now() + 1s));
        std::vector<NodeId> rest;
        for (NodeId id : c.ids()) if (id != leader) rest.push_back(id);
        c.sim().faults().partitions = {{leader}, rest};
        c.sim().schedule(c.sim().now() + 350ms, "heal", [&c] { c.sim().faults().partitions.clear(); });

        ASSERT_TRUE(c.sim().run_until([&] { return done.has_value(); }, c.sim().now() + 3s)) << c.context();
        EXPECT_EQ(c.client(101).stats().timeouts, 0u) << c.context();
        EXPECT_GE(c.client(101).stats().not_leader, 1u);
        EXPECT_SAFE(c);
    }
}

// Several clients append under random crashes, partitions, pauses and loss. Every client's
// own key must end up holding exactly its acknowledged appends, each once and in order; the
// shared key must hold every acknowledged append exactly once. (Phase 6 checks full
// linearizability; this checks the dedup and no-loss parts of it directly.)
TEST(Kv, ChaosWithClientsNeverLosesOrDuplicatesAnAppend) {
    for (std::uint64_t seed : seeds(50)) {
        RaftCluster c(seed, 5, ClusterOptions{.kv = true, .clients = 3});
        c.quiet();
        c.schedule_chaos(seed, 10s);

        std::map<NodeId, std::string> acked_own;          // what each client's key must be
        std::map<NodeId, std::vector<std::string>> acked_shared;
        std::map<NodeId, int> next;
        std::function<void(NodeId)> drive = [&](NodeId id) {
            if (c.sim().now() >= raft::Time{10s}) return;
            const int n = next[id]++;
            const std::string piece = std::format("{}:{},", id, n);
            if (n % 2 == 0) {
                c.client(id).append(std::format("k{}", id), piece, [&, id, piece](const Result&) {
                    acked_own[id] += piece;
                    drive(id);
                });
            } else {
                c.client(id).append("shared", piece, [&, id, piece](const Result&) {
                    acked_shared[id].push_back(piece);
                    drive(id);
                });
            }
        };
        c.sim().start();
        for (NodeId id : c.client_ids()) drive(id);
        c.sim().run_until(raft::Time{10s});
        ASSERT_TRUE(c.violations().empty()) << testing::PrintToString(c.violations()) << c.context();

        // Everything heals at 10s; clients keep retrying until their last request lands.
        ASSERT_TRUE(c.sim().run_until([&] {
            for (NodeId id : c.client_ids()) if (c.client(id).busy()) return false;
            return true;
        }, raft::Time{25s})) << c.context();
        ASSERT_TRUE(c.wait_for_convergence(5s)) << c.context();

        const kv::StateMachine& sm = c.server(c.ids()[0])->state();
        std::string shared = sm.get("shared").value_or("");
        for (NodeId id : c.client_ids()) {
            EXPECT_EQ(sm.get(std::format("k{}", id)).value_or(""), acked_own[id]) << "client " << id;
            // In the shared key, this client's pieces appear once each, in the order acked.
            std::size_t pos = 0;
            for (const std::string& piece : acked_shared[id]) {
                const std::size_t at = shared.find(piece, pos);
                ASSERT_NE(at, std::string::npos) << "lost or reordered: " << piece;
                EXPECT_EQ(shared.find(piece, at + 1), std::string::npos) << "duplicated: " << piece;
                pos = at + piece.size();
            }
        }
        std::size_t total = 0;
        for (const auto& [id, pieces] : acked_shared) for (const auto& p : pieces) total += p.size();
        EXPECT_EQ(shared.size(), total) << "the shared key holds something no client was told about";
        EXPECT_SAFE(c);
    }
}

}  // namespace
}  // namespace raftkv
