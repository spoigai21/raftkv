// Phase 7: the fault matrix (implementation guide). One test per row.
//
// Every row runs a 3-server KV cluster with 3 clients doing get/put/append, injects its
// fault, checks the row's own expectation, then heals and requires the cluster to converge.
// The Raft invariants are checked after every event throughout, and each run's history is
// written for the linearizability checker (tests/lincheck/run.sh), which is what proves
// "no acknowledged write lost": a lost write makes a later read impossible to explain.
//
// Rows with a real-process counterpart are also covered by tests/cluster/smoke.sh.

#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>
#include <iterator>
#include <string_view>

#include "temp_dir.hpp"
#include "test_seeds.hpp"
#include "workload.hpp"

namespace raftkv {
namespace {

using namespace std::chrono_literals;
using raft::NodeId;
using raft::Role;
using raft::Time;
using test::ClusterOptions;
using test::RaftCluster;
using test::Workload;

#define EXPECT_SAFE(c) EXPECT_TRUE((c).violations().empty()) \
    << testing::PrintToString((c).violations()) << "\n" << (c).context()

constexpr Time kFaultAt{1s};       // the workload has warmed up by then
constexpr Time kStopIssuing{6s};   // clients start no new operations after this

double ms(raft::Duration d) { return static_cast<double>(d.count()) / 1000.0; }

double median_ms(const std::vector<raft::Duration>& sorted) {
    return sorted.empty() ? 0.0 : ms(sorted[sorted.size() / 2]);
}

// One row's run: the cluster, its workload and (for rows that need disks) a data directory.
struct MatrixRun {
    explicit MatrixRun(std::uint64_t seed, bool files = false) : seed(seed), c(seed, 3, ClusterOptions{.kv = true, .clients = 3}) {
        c.quiet();
        if (files) {
            dir.emplace();
            c.use_file_storage(dir->path());
        }
        c.sim().start();
        w = test::run_workload(c, seed, kStopIssuing);
    }

    // Runs up to the fault time and returns the leader then.
    NodeId leader_at_fault_time() {
        c.sim().run_until(kFaultAt);
        EXPECT_TRUE(c.wait_for_leader(2s)) << c.context();
        return *c.leader();
    }

    NodeId a_follower_of(NodeId leader) const { return leader == 1 ? 2 : 1; }

    // Heals everything, lets every client finish, reads every key once more, and requires
    // convergence (of the nodes that are up, if `allow_down`). Writes the history out.
    void finish(std::string_view row, bool allow_down = false) {
        auto& f = c.sim().faults();
        f.partitions.clear();
        f.drop_rate = 0.0;
        f.link_delay.clear();
        for (NodeId id : c.ids()) if (c.sim().is_paused(id)) c.sim().resume(id);

        ASSERT_TRUE(test::wait_for_clients_idle(c, Time{30s})) << row << "\n" << c.context();
        for (const char* key : test::kWorkloadKeys) {   // final reads see every acknowledged write
            const auto i = w.history->invoke(101, kv::Op::Get, key, "", c.sim().now());
            bool done = false;
            c.client(101).get(key, [&](const kv::Result& r) {
                w.history->complete(i, r, c.sim().now());
                done = true;
            });
            ASSERT_TRUE(c.sim().run_until([&] { return done; }, c.sim().now() + 10s)) << c.context();
        }
        ASSERT_TRUE(c.wait_for_convergence(5s, allow_down)) << row << "\n" << c.context();
        EXPECT_SAFE(c);
        w.history->write_if_requested(seed, std::format("matrix-{}", row));
    }

    std::uint64_t seed;
    std::optional<test::TempDir> dir;
    RaftCluster c;
    Workload w;
};

raft::Term highest_term(RaftCluster& c) {
    raft::Term t = 0;
    for (NodeId id : c.ids()) if (auto* r = c.raft(id)) t = std::max(t, r->current_term());
    return t;
}

void edit_file(const std::filesystem::path& p, const std::function<void(std::vector<char>&)>& fn) {
    std::vector<char> bytes;
    {
        std::ifstream in(p, std::ios::binary);
        bytes.assign(std::istreambuf_iterator<char>(in), {});
    }
    fn(bytes);
    std::ofstream(p, std::ios::binary | std::ios::trunc).write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
}

// ---- 1. Leader crash mid-write ------------------------------------------------------------
// Kill the leader after it has sent AppendEntries for an entry, before that entry commits.
// Expected: a new leader within about a second; no acknowledged write lost.
TEST(FaultMatrix, LeaderCrashMidWrite) {
    std::vector<raft::Duration> failovers;
    for (std::uint64_t seed : test::chaos_seeds(20)) {
        MatrixRun r(seed);
        r.c.sim().run_until(kFaultAt);
        // The instant some leader holds an entry it has sent but not yet committed.
        NodeId victim = 0;
        ASSERT_TRUE(r.c.sim().run_until([&] {
            for (NodeId id : r.c.ids()) {
                auto* n = r.c.raft(id);
                if (n && n->role() == Role::Leader && n->last_log_index() > n->commit_index()) {
                    victim = id;
                    return true;
                }
            }
            return false;
        }, Time{3s})) << r.c.context();
        const Time crashed = r.c.sim().now();
        r.c.sim().crash(victim);
        ASSERT_TRUE(r.c.sim().run_until([&] {
            auto l = r.c.leader();
            return l && *l != victim;
        }, crashed + 3s)) << r.c.context();
        failovers.push_back(r.c.sim().now() - crashed);
        EXPECT_LT(r.c.sim().now() - crashed, 1s) << "seed " << seed;
        r.c.sim().restart(victim);
        r.finish("leader-crash");
    }
    std::sort(failovers.begin(), failovers.end());
    std::printf("[ matrix ] leader failover over %zu seeds: median %.0f ms, max %.0f ms\n", failovers.size(),
                median_ms(failovers), ms(failovers.back()));
}

// ---- 2. Minority partition ----------------------------------------------------------------
// Cut one server off (the leader in half the seeds, a follower in the rest).
// Expected: the majority keeps serving; the cut-off node commits, and so acknowledges, nothing.
TEST(FaultMatrix, MinorityPartition) {
    for (std::uint64_t seed : test::chaos_seeds(20)) {
        MatrixRun r(seed);
        const NodeId leader = r.leader_at_fault_time();
        const NodeId cut = seed % 2 ? leader : r.a_follower_of(leader);
        std::vector<NodeId> rest;
        for (NodeId id : r.c.ids()) if (id != cut) rest.push_back(id);
        const raft::Index commit_before = r.c.raft(cut)->commit_index();
        r.c.sim().faults().partitions = {{cut}, rest};
        const Time from = r.c.sim().now();
        r.c.sim().run_until(from + 3s);

        EXPECT_EQ(r.c.raft(cut)->commit_index(), commit_before)
            << "the cut-off node committed something on its own (seed " << seed << ")";
        EXPECT_GT(r.w.history->returned_between(from + 1s, from + 3s), 20u)
            << "the majority stopped serving (seed " << seed << ")\n" << r.c.context();
        r.finish("minority-partition");
    }
}

// ---- 3. Majority partition ----------------------------------------------------------------
// Cut every server off from every other: no side has a majority.
// Expected: unavailable, not inconsistent. Nothing commits and no operation completes;
// once healed, service resumes and the history is still linearizable.
TEST(FaultMatrix, MajorityPartition) {
    for (std::uint64_t seed : test::chaos_seeds(20)) {
        MatrixRun r(seed);
        r.leader_at_fault_time();
        std::map<NodeId, raft::Index> commit_before;
        for (NodeId id : r.c.ids()) commit_before[id] = r.c.raft(id)->commit_index();
        r.c.sim().faults().partitions = {{1}, {2}, {3}};
        const Time from = r.c.sim().now();
        r.c.sim().run_until(from + 2s);

        for (NodeId id : r.c.ids()) {
            EXPECT_EQ(r.c.raft(id)->commit_index(), commit_before[id]) << "node " << id << " committed without a majority";
        }
        // Replies already on their way at the cut are allowed to land (delays are <= 10 ms).
        EXPECT_EQ(r.w.history->returned_between(from + 50ms, from + 2s), 0u)
            << "an operation completed with no majority anywhere (seed " << seed << ")";
        r.finish("majority-partition");
    }
}

// ---- 4. Follower crash + restart ----------------------------------------------------------
// kill -9 a follower (on real files), restart it a second later.
// Expected: it recovers from its own log and catches up with what it missed.
TEST(FaultMatrix, FollowerCrashAndRestart) {
    for (std::uint64_t seed : test::chaos_seeds(20)) {
        MatrixRun r(seed, /*files=*/true);
        const NodeId f = r.a_follower_of(r.leader_at_fault_time());
        r.c.sim().crash(f);
        r.c.sim().run_until(r.c.sim().now() + 1s);
        r.c.sim().restart(f);
        ASSERT_TRUE(r.c.sim().is_up(f)) << r.c.sim().boot_error(f);
        r.finish("follower-crash");   // converged: f holds the full log again
    }
}

// ---- 5. Slow follower ---------------------------------------------------------------------
// 500 ms extra delay on one follower's link, compared with the same seed without it.
// The plan's row says "p50 unchanged". Measured, it depends on which direction is slow:
//   a. only its replies to the leader are slow: stable, the median rises by about 12%;
//   b. both directions: the delay exceeds the election timeout, so the slow follower keeps
//      timing out. Before PreVote it then forced an election every time (98 over 20 seeds, up
//      to 22% throughput lost). With PreVote the others, still hearing the leader, refuse.
struct SlowResult {
    double p50_ms = 0;
    std::size_t ops = 0;
    raft::Term extra_terms = 0;
};

SlowResult slow_follower_run(std::uint64_t seed, int directions /* 0, 1 or 2 */, std::string_view row) {
    MatrixRun r(seed);
    const NodeId leader = r.leader_at_fault_time();
    const NodeId f = r.a_follower_of(leader);
    const raft::Term before = highest_term(r.c);
    if (directions >= 1) r.c.sim().faults().link_delay[{f, leader}] = 500ms;
    if (directions >= 2) r.c.sim().faults().link_delay[{leader, f}] = 500ms;
    r.c.sim().run_until(kStopIssuing);
    const auto lat = r.w.history->latencies(kFaultAt + 500ms, kStopIssuing);
    SlowResult out{.p50_ms = median_ms(lat), .ops = lat.size(), .extra_terms = highest_term(r.c) - before};
    r.finish(row);
    return out;
}

// Prints the median and worst change across seeds: "p50 +x% (worst +y%), ops -z% (worst -w%)".
void report_slow(const char* what, std::vector<double> p50_change, std::vector<double> ops_change) {
    std::sort(p50_change.begin(), p50_change.end());
    std::sort(ops_change.begin(), ops_change.end());
    std::printf("[ matrix ] %s over %zu seeds: p50 %+.0f%% (worst %+.0f%%), ops %+.0f%% (worst %+.0f%%)\n", what,
                p50_change.size(), p50_change[p50_change.size() / 2], p50_change.back(),
                ops_change[ops_change.size() / 2], ops_change.front());
}

TEST(FaultMatrix, SlowFollowerReplies) {
    std::vector<double> p50_change, ops_change;
    for (std::uint64_t seed : test::chaos_seeds(20)) {
        const SlowResult base = slow_follower_run(seed, 0, "slow-baseline");
        const SlowResult slow = slow_follower_run(seed, 1, "slow-replies");
        EXPECT_LE(slow.p50_ms, base.p50_ms * 1.25) << "seed " << seed;
        EXPECT_EQ(slow.extra_terms, 0u) << "seed " << seed << ": a slow ack should not cause elections";
        EXPECT_GE(slow.ops, base.ops * 8 / 10) << "seed " << seed;
        p50_change.push_back(100.0 * (slow.p50_ms / base.p50_ms - 1));
        ops_change.push_back(100.0 * (static_cast<double>(slow.ops) / static_cast<double>(base.ops) - 1));
    }
    report_slow("slow replies", p50_change, ops_change);
}

TEST(FaultMatrix, SlowFollowerLinkBothWays) {
    raft::Term elections = 0;
    std::vector<double> p50_change, ops_change;
    for (std::uint64_t seed : test::chaos_seeds(20)) {
        const SlowResult base = slow_follower_run(seed, 0, "slow-baseline");
        const SlowResult slow = slow_follower_run(seed, 2, "slow-both-ways");
        EXPECT_LE(slow.p50_ms, base.p50_ms * 1.25) << "seed " << seed;
        EXPECT_GE(slow.ops, base.ops * 7 / 10) << "seed " << seed << ": the cluster should stay available";
        elections += slow.extra_terms;
        p50_change.push_back(100.0 * (slow.p50_ms / base.p50_ms - 1));
        ops_change.push_back(100.0 * (static_cast<double>(slow.ops) / static_cast<double>(base.ops) - 1));
    }
    report_slow("slow link both ways", p50_change, ops_change);
    std::printf("[ matrix ] slow link both ways: %llu extra elections in total\n",
                static_cast<unsigned long long>(elections));
    // PreVote: the slow follower may ask, but the others still hear the leader and refuse.
    EXPECT_EQ(elections, 0u) << "a slow follower forced elections: is PreVote working?";
}

// ---- 6. Message drops ---------------------------------------------------------------------
// Drop 20% of all messages, servers and clients alike, for five seconds.
// Expected: progress continues through retries. It does, for every client, but slowly. Each
// lost request or reply costs the client a full 500 ms timeout (implementation guide §3.5),
// after which it tries the next server, usually a follower that redirects it back: two more
// lossy hops. Throughput falls to about 5% of normal, and in a few seeds a whole second
// passes with no completion. Phase 9 measures this (prediction 4) before any client tuning.
TEST(FaultMatrix, TwentyPercentMessageDrops) {
    std::size_t lossy_ops = 0, clean_ops = 0;
    for (std::uint64_t seed : test::chaos_seeds(20)) {
        for (double drop : {0.0, 0.2}) {
            MatrixRun r(seed);
            r.leader_at_fault_time();
            r.c.sim().faults().drop_rate = drop;
            const Time from = r.c.sim().now();
            r.c.sim().run_until(kStopIssuing);
            // Every client completes operations during the lossy window, and the cluster as a
            // whole does in each half of it. (Not "every client in every second": a client can
            // lose five attempts in a row, about an 8% chance per 2.5 s at this loss rate.)
            for (NodeId id : r.c.client_ids()) {
                EXPECT_GT(r.w.history->returned_between(from, kStopIssuing, id), 0u)
                    << "client " << id << " made no progress (seed " << seed << ", drop " << drop << ")";
            }
            const Time mid = from + (kStopIssuing - from) / 2;
            EXPECT_GT(r.w.history->returned_between(from, mid), 0u) << "seed " << seed;
            EXPECT_GT(r.w.history->returned_between(mid, kStopIssuing), 0u) << "seed " << seed;
            (drop > 0 ? lossy_ops : clean_ops) += r.w.history->returned_between(from, kStopIssuing);
            r.finish(drop > 0 ? "drops-20pct" : "drops-baseline");
        }
    }
    std::printf("[ matrix ] 20%% drops: %zu ops completed vs %zu with no loss (%.1f%%)\n", lossy_ops, clean_ops,
                100.0 * static_cast<double>(lossy_ops) / static_cast<double>(clean_ops));
}

// ---- 7. Paused leader ---------------------------------------------------------------------
// SIGSTOP the leader for a second.
// Expected: the others elect a new leader; the old one steps down when it resumes.
TEST(FaultMatrix, PausedLeader) {
    for (std::uint64_t seed : test::chaos_seeds(20)) {
        MatrixRun r(seed);
        const NodeId old = r.leader_at_fault_time();
        r.c.sim().pause(old);
        ASSERT_TRUE(r.c.sim().run_until([&] {
            auto l = r.c.leader();
            return l && *l != old;
        }, r.c.sim().now() + 1s)) << r.c.context();
        r.c.sim().run_until(r.c.sim().now() + 500ms);
        r.c.sim().resume(old);
        r.c.sim().run_until(r.c.sim().now() + 500ms);
        EXPECT_EQ(r.c.raft(old)->role(), Role::Follower) << "seed " << seed;
        r.finish("paused-leader");
    }
}

// ---- 8. Torn log tail ---------------------------------------------------------------------
// A follower dies mid-append, leaving half a record at the end of its log.
// Expected: it cuts off the unsynced record, rejoins and catches up.
TEST(FaultMatrix, TornLogTail) {
    for (std::uint64_t seed : test::chaos_seeds(20)) {
        MatrixRun r(seed, /*files=*/true);
        const NodeId f = r.a_follower_of(r.leader_at_fault_time());
        r.c.sim().crash(f);
        edit_file(r.dir->path() / std::format("node-{}/log", f),
                  [](std::vector<char>& b) { b.insert(b.end(), {'\x40', '\x00', '\x00', '\x00', '\x13'}); });
        r.c.sim().run_until(r.c.sim().now() + 200ms);
        r.c.sim().restart(f);
        ASSERT_TRUE(r.c.sim().is_up(f)) << r.c.sim().boot_error(f);
        r.finish("torn-tail");
    }
}

// ---- 9. Corrupt log tail ------------------------------------------------------------------
// Flip a byte inside a complete, synced record of a follower's log.
// Expected: it refuses to start, so it can serve nothing stale; the other two carry on.
TEST(FaultMatrix, CorruptLogTail) {
    for (std::uint64_t seed : test::chaos_seeds(20)) {
        MatrixRun r(seed, /*files=*/true);
        const NodeId f = r.a_follower_of(r.leader_at_fault_time());
        r.c.sim().crash(f);
        edit_file(r.dir->path() / std::format("node-{}/log", f), [](std::vector<char>& b) { b[b.size() - 20] ^= 0x08; });
        r.c.sim().restart(f);
        EXPECT_FALSE(r.c.sim().is_up(f)) << "seed " << seed;
        EXPECT_NE(r.c.sim().boot_error(f).find("corrupt"), std::string::npos) << r.c.sim().boot_error(f);
        const Time from = r.c.sim().now();
        r.c.sim().run_until(kStopIssuing);
        EXPECT_GT(r.w.history->returned_between(from, kStopIssuing), 50u) << "the other two stopped serving";
        r.finish("corrupt-tail", /*allow_down=*/true);
        EXPECT_FALSE(r.c.sim().is_up(f));
    }
}

}  // namespace
}  // namespace raftkv
