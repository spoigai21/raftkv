// Phase 8: snapshots and log compaction.

#include <gtest/gtest.h>

#include <cstdio>
#include <filesystem>
#include <fstream>

#include "kv/state_machine.hpp"
#include "raft_cluster.hpp"
#include "sim/sim_storage.hpp"
#include "sim_test_nodes.hpp"
#include "store/file_storage.hpp"
#include "temp_dir.hpp"
#include "test_seeds.hpp"
#include "workload.hpp"

namespace raftkv {
namespace {

namespace fs = std::filesystem;
using namespace std::chrono_literals;
using raft::LogEntry;
using raft::NodeId;
using raft::Snapshot;
using test::ClusterOptions;
using test::RaftCluster;
using test::TempDir;

std::vector<LogEntry> entries(raft::Index from, raft::Index to, raft::Term term) {
    std::vector<LogEntry> out;
    for (raft::Index i = from; i <= to; ++i) out.push_back({term, i, "cmd" + std::to_string(i)});
    return out;
}

#define EXPECT_SAFE(c) EXPECT_TRUE((c).violations().empty()) \
    << testing::PrintToString((c).violations()) << "\n" << (c).context()

// ---- the KV state machine's snapshot -----------------------------------------------------

TEST(KvSnapshot, RoundTripsDataAndDedupTable) {
    kv::StateMachine sm;
    sm.apply({.client_id = 7, .seq = 1, .op = kv::Op::Put, .key = "a", .value = "1"});
    sm.apply({.client_id = 7, .seq = 2, .op = kv::Op::Append, .key = "a", .value = "2"});
    sm.apply({.client_id = 9, .seq = 1, .op = kv::Op::Put, .key = "b", .value = std::string("\0x", 2)});
    auto restored = kv::StateMachine::deserialize(sm.serialize());
    ASSERT_TRUE(restored.has_value()) << restored.error();
    EXPECT_EQ(*restored, sm);
    EXPECT_EQ(restored->serialize(), sm.serialize()) << "equal states must encode to equal bytes";
}

// The reason the dedup table is in the snapshot: a retry that arrives after a node was
// rebuilt from a snapshot must still be recognised as a duplicate.
TEST(KvSnapshot, RestoredStateStillSuppressesDuplicates) {
    kv::StateMachine sm;
    sm.apply({.client_id = 7, .seq = 1, .op = kv::Op::Append, .key = "k", .value = "x"});
    auto restored = kv::StateMachine::deserialize(sm.serialize());
    ASSERT_TRUE(restored.has_value());
    restored->apply({.client_id = 7, .seq = 1, .op = kv::Op::Append, .key = "k", .value = "x"});
    EXPECT_EQ(restored->get("k"), "x");
}

TEST(KvSnapshot, GarbageIsRejected) {
    EXPECT_FALSE(kv::StateMachine::deserialize("\xff\xff\xff").has_value());
}

// ---- storage ------------------------------------------------------------------------------

TEST(SimStorageSnapshot, DropsCoveredEntriesAndSurvivesACrash) {
    sim::SimStorage s;
    sim::Rng rng(1);
    s.append(entries(1, 10, 1));
    s.sync();
    s.append(entries(11, 12, 1));   // unsynced
    s.save_snapshot({.last_included_index = 6, .last_included_term = 1, .data = "state@6"});
    EXPECT_EQ(s.load().log, entries(7, 12, 1));
    s.crash(sim::SimStorage::CrashMode::DropUnsynced, rng);
    EXPECT_EQ(s.load().snapshot->data, "state@6");
    EXPECT_EQ(s.load().log, entries(7, 10, 1));
}

TEST(FileStorageSnapshot, ReopenRecoversSnapshotAndTheEntriesAfterIt) {
    TempDir dir;
    raft::PersistentState want;
    std::uintmax_t before = 0;
    {
        auto s = *store::FileStorage::open(dir.path());
        s->save_hard_state(3, 1);
        s->append(entries(1, 200, 1));
        s->sync();
        before = fs::file_size(dir / "log");
        s->save_snapshot({.last_included_index = 190, .last_included_term = 1, .data = "state@190"});
        s->append(entries(201, 202, 2));
        s->sync();
        want = s->load();
    }
    EXPECT_LT(fs::file_size(dir / "log"), before / 10) << "the log was not compacted";
    auto s = store::FileStorage::open(dir.path());
    ASSERT_TRUE(s.has_value()) << s.error();
    EXPECT_EQ((*s)->load(), want);
    ASSERT_TRUE(want.snapshot.has_value());
    EXPECT_EQ(want.log.front().index, 191u);
}

// A crash after the snapshot is saved but before the log is rewritten leaves the new snapshot
// next to the old, full log. Recovery must drop what the snapshot covers.
TEST(FileStorageSnapshot, CrashBetweenSnapshotAndLogRewriteRecovers) {
    TempDir done, crashed;
    {
        auto a = *store::FileStorage::open(done.path());
        a->append(entries(1, 20, 1));
        a->sync();
    }
    fs::copy(done.path(), crashed.path(), fs::copy_options::recursive | fs::copy_options::overwrite_existing);
    {
        auto a = *store::FileStorage::open(done.path());
        a->save_snapshot({.last_included_index = 12, .last_included_term = 1, .data = "state@12"});
    }
    fs::copy_file(done / "snapshot", crashed / "snapshot");   // the old log, the new snapshot
    std::ofstream(crashed / "log.tmp") << "half-written rewrite";
    auto recovered = store::FileStorage::open(crashed.path());
    ASSERT_TRUE(recovered.has_value()) << recovered.error();
    auto expected = store::FileStorage::open(done.path());
    EXPECT_EQ((*recovered)->load(), (*expected)->load());
    EXPECT_FALSE(fs::exists(crashed / "log.tmp"));
}

TEST(FileStorageSnapshot, CorruptSnapshotRefusesToOpen) {
    TempDir dir;
    {
        auto s = *store::FileStorage::open(dir.path());
        s->append(entries(1, 5, 1));
        s->save_snapshot({.last_included_index = 3, .last_included_term = 1, .data = "state@3"});
    }
    {
        std::fstream f(dir / "snapshot", std::ios::in | std::ios::out | std::ios::binary);
        f.seekp(10);
        f.put('\x55');
    }
    auto s = store::FileStorage::open(dir.path());
    ASSERT_FALSE(s.has_value());
    EXPECT_NE(s.error().find("snapshot"), std::string::npos) << s.error();
}

TEST(FileStorageSnapshot, LogStartingAfterAGapIsRejected) {
    TempDir dir;
    {
        auto s = *store::FileStorage::open(dir.path());
        s->append(entries(1, 10, 1));
        s->save_snapshot({.last_included_index = 8, .last_included_term = 1, .data = "s"});
    }
    fs::remove(dir / "snapshot");   // now the log starts at 9 with nothing before it
    auto s = store::FileStorage::open(dir.path());
    ASSERT_FALSE(s.has_value());
    EXPECT_NE(s.error().find("gap"), std::string::npos) << s.error();
}

// ---- InstallSnapshot on a follower, driven by hand ----------------------------------------

// Node 1 is a Raft follower with a preloaded log; node 2 is a probe playing the leader.
struct FollowerRig {
    explicit FollowerRig(std::vector<LogEntry> preload)
        : sim(1, {1, 2, 3}, [this](NodeId id, raft::Env& env) -> std::unique_ptr<raft::Node> {
              if (id != 1) return std::make_unique<test::ProbeNode>(env);
              return std::make_unique<raft::Raft>(
                  raft::RaftConfig{.id = 1, .peers = {2, 3}}, env, [](const LogEntry&) {},
                  raft::SnapshotHooks{.take = [] { return std::string(); },
                                      .restore = [this](const std::string& d) { restored = d; }});
          }) {
        sim.describe_messages_with(raft::describe);
        sim.storage(1).append(preload);
        sim.storage(1).sync();
        sim.start();
    }

    raft::InstallSnapshotReply install(raft::Term term, Snapshot s) {
        auto& probe = sim.node_as<test::ProbeNode>(2);
        probe.env().send(raft::encode(1, raft::InstallSnapshot{.term = term, .leader_id = 2, .snapshot = std::move(s)}));
        sim.run_for(5ms);
        return std::get<raft::InstallSnapshotReply>(*raft::parse(probe.inbox.back()));
    }

    raft::Raft& node() { return sim.node_as<raft::Raft>(1); }
    sim::Sim sim;
    std::string restored;
};

TEST(InstallSnapshot, MatchingLastEntryKeepsTheLogAfterIt) {
    FollowerRig rig(entries(1, 10, 1));
    const auto reply = rig.install(1, {.last_included_index = 6, .last_included_term = 1, .data = "S6"});
    EXPECT_EQ(reply.match_index, 6u);
    EXPECT_EQ(rig.restored, "S6");
    EXPECT_EQ(rig.node().snapshot_index(), 6u);
    EXPECT_EQ(rig.node().last_log_index(), 10u) << "entries 7..10 should have been kept";
    EXPECT_EQ(rig.node().last_applied(), 6u);
    EXPECT_EQ(rig.sim.storage(1).load().log, entries(7, 10, 1));
}

TEST(InstallSnapshot, ConflictingLastEntryDiscardsTheWholeLog) {
    FollowerRig rig(entries(1, 10, 1));
    rig.install(2, {.last_included_index = 7, .last_included_term = 2, .data = "S7"});
    EXPECT_EQ(rig.node().snapshot_index(), 7u);
    EXPECT_EQ(rig.node().last_log_index(), 7u) << "a log that disagrees at 7 must not survive";
    EXPECT_TRUE(rig.sim.storage(1).load().log.empty());
    EXPECT_EQ(rig.node().current_term(), 2u);
}

TEST(InstallSnapshot, StaleSnapshotChangesNothing) {
    FollowerRig rig(entries(1, 10, 1));
    rig.install(1, {.last_included_index = 8, .last_included_term = 1, .data = "S8"});
    rig.restored.clear();
    const auto reply = rig.install(1, {.last_included_index = 4, .last_included_term = 1, .data = "S4"});
    EXPECT_EQ(reply.match_index, 4u);
    EXPECT_TRUE(rig.restored.empty()) << "an older snapshot must not replace newer state";
    EXPECT_EQ(rig.node().snapshot_index(), 8u);
}

TEST(InstallSnapshot, OldTermIsRejected) {
    FollowerRig rig(entries(1, 3, 1));
    rig.install(5, {.last_included_index = 2, .last_included_term = 1, .data = "S2"});
    const auto reply = rig.install(4, {.last_included_index = 3, .last_included_term = 1, .data = "S3"});
    EXPECT_EQ(reply.term, 5u);
    EXPECT_EQ(rig.node().snapshot_index(), 2u);
}

// ---- snapshots hold exactly the state they are labelled with -------------------------------

// A state machine that only counts applied entries makes the label checkable on its own:
// a snapshot labelled N must contain N. (In the KV store, re-applying an entry is usually
// absorbed by duplicate detection, so a mislabelled snapshot can go unnoticed there.)
TEST(Snapshots, SnapshotHoldsExactlyTheStateAtItsLabel) {
    raft::Index applied = 0;
    sim::Sim sim(1, {1}, [&applied](NodeId, raft::Env& env) {
        raft::RaftConfig cfg{.id = 1, .peers = {}};
        cfg.snapshot_every = 5;
        return std::make_unique<raft::Raft>(
            cfg, env,
            [&applied](const LogEntry& e) {
                EXPECT_EQ(e.index, applied + 1) << "applied out of order, or twice";
                applied = e.index;
            },
            raft::SnapshotHooks{.take = [&applied] { return std::to_string(applied); },
                                .restore = [&applied](const std::string& d) { applied = std::stoull(d); }});
    });
    sim.start();
    ASSERT_TRUE(sim.run_until([&] { return sim.node_as<raft::Raft>(1).role() == raft::Role::Leader; }, raft::Time{1s}));
    for (int i = 0; i < 23; ++i) sim.node_as<raft::Raft>(1).propose("x");
    sim.run_for(10ms);

    const auto snap = sim.storage(1).load().snapshot;
    ASSERT_TRUE(snap.has_value());
    EXPECT_EQ(snap->data, std::to_string(snap->last_included_index)) << "snapshot contents do not match its label";

    // After a restart the state comes back from the snapshot and replay resumes right after it.
    const raft::Index last = sim.node_as<raft::Raft>(1).last_log_index();
    sim.crash(1);
    applied = 0;
    sim.restart(1);
    EXPECT_EQ(applied, snap->last_included_index);
    ASSERT_TRUE(sim.run_until([&] { return sim.node(1) && sim.node_as<raft::Raft>(1).role() == raft::Role::Leader; },
                              sim.now() + 1s));
    sim.run_for(10ms);
    EXPECT_GE(applied, last);
}

// ---- the Phase 8 done-gate -----------------------------------------------------------------

// 100k operations (a fifth under sanitizers) on real files with snapshots every 200 entries:
// every node's log, in memory and on disk, stays bounded the whole way.
TEST(Snapshots, LogStaysBoundedOverAHundredThousandOperations) {
    constexpr raft::Index kEvery = 200;
    const std::size_t target = test::chaos_seed_count(100'000);
    TempDir dir;
    RaftCluster c(1, 3, ClusterOptions{.kv = true, .clients = 8, .snapshot_every = kEvery});
    c.quiet();
    c.use_file_storage(dir.path());
    c.sim().start();
    test::Workload w = test::run_workload(c, 1, raft::Time{24h});

    std::size_t max_entries = 0;
    std::uintmax_t max_bytes = 0;
    for (std::size_t done = 0; done < target;) {
        // At ~1,000 ops per virtual second this needs ~100 s; a stalled cluster fails here
        // instead of spinning forever.
        ASSERT_LT(c.sim().now(), raft::Time{1h}) << "only " << done << " ops completed\n" << c.context();
        c.sim().run_for(1s);
        for (NodeId id : c.ids()) {
            max_entries = std::max<std::size_t>(max_entries, c.raft(id)->last_log_index() - c.raft(id)->snapshot_index());
            max_bytes = std::max(max_bytes, fs::file_size(dir / std::format("node-{}/log", id)));
        }
        done = w.history->returned_between(raft::Time{}, c.sim().now());
        ASSERT_TRUE(c.violations().empty()) << testing::PrintToString(c.violations());
    }
    std::printf("[ snapshots ] %zu ops: log at most %zu entries, %ju bytes on disk; snapshot index %llu\n", target,
                max_entries, static_cast<std::uintmax_t>(max_bytes),
                static_cast<unsigned long long>(c.raft(1)->snapshot_index()));
    // Up to one interval of not-yet-snapshotted entries, plus those not yet applied.
    EXPECT_LE(max_entries, 2 * kEvery);
    EXPECT_LE(max_bytes, 2 * kEvery * 128);   // records here are under 128 bytes each
    EXPECT_GT(c.raft(1)->snapshot_index(), target / 2) << "snapshots should have kept up";
}

// A node that has been down since the start joins a cluster that has already compacted its
// log away: it can only catch up by InstallSnapshot, and must do so in under a second.
TEST(Snapshots, FreshNodeCatchesUpBySnapshotInUnderASecond) {
    std::vector<raft::Duration> times;
    for (std::uint64_t seed : test::seeds(10)) {
        RaftCluster c(seed, 3, ClusterOptions{.kv = true, .clients = 3, .snapshot_every = 100});
        c.quiet();
        c.sim().start();
        c.sim().crash(3);   // never really ran: its storage is empty
        test::Workload w = test::run_workload(c, seed, raft::Time{24h});
        c.sim().run_until(raft::Time{10s});
        ASSERT_GT(c.raft(1)->snapshot_index(), 0u);

        const raft::Index target = std::max(c.raft(1)->commit_index(), c.raft(2)->commit_index());
        const raft::Time start = c.sim().now();
        c.sim().restart(3);
        ASSERT_TRUE(c.sim().run_until([&] { return c.raft(3)->last_applied() >= target; }, start + 5s))
            << c.context();
        times.push_back(c.sim().now() - start);
        EXPECT_LT(c.sim().now() - start, 1s) << "seed " << seed;
        EXPECT_GT(c.raft(3)->snapshot_index(), 0u) << "it should have caught up by snapshot";
        EXPECT_SAFE(c);
    }
    std::sort(times.begin(), times.end());
    std::printf("[ snapshots ] fresh node caught up in %.0f ms median, %.0f ms max\n",
                static_cast<double>(times[times.size() / 2].count()) / 1000.0,
                static_cast<double>(times.back().count()) / 1000.0);
}

// Random faults with snapshots every 30 entries, on real files: crashes now restart from a
// snapshot, lagging followers get InstallSnapshot, and every invariant still holds.
TEST(Snapshots, ChaosOnRealFilesWithFrequentSnapshots) {
    for (std::uint64_t seed : test::chaos_seeds(30)) {
        TempDir dir;
        RaftCluster c(seed, 3, ClusterOptions{.kv = true, .clients = 3, .snapshot_every = 30});
        c.quiet();
        c.use_file_storage(dir.path());
        c.schedule_chaos(seed, 8s);
        c.sim().start();
        test::Workload w = test::run_workload(c, seed, raft::Time{8s});
        c.sim().run_until(raft::Time{8s});
        ASSERT_TRUE(c.violations().empty()) << testing::PrintToString(c.violations()) << c.context();
        ASSERT_TRUE(test::wait_for_clients_idle(c, raft::Time{30s})) << c.context();
        ASSERT_TRUE(c.wait_for_convergence(5s)) << c.context();
        EXPECT_SAFE(c);
    }
}

}  // namespace
}  // namespace raftkv
