#include "sim/sim.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "sim_test_nodes.hpp"
#include "test_seeds.hpp"

namespace raftkv {
namespace {

using namespace std::chrono_literals;
using raft::Duration;
using raft::NodeId;
using raft::Time;
using sim::Sim;
using test::GossipNode;
using test::ProbeNode;
using test::seeds;

Time at(Duration d) { return Time{d}; }

Sim::NodeFactory probes() {
    return [](NodeId, raft::Env& env) { return std::make_unique<ProbeNode>(env); };
}

// Five gossiping nodes under every fault the simulator has: loss, jitter, reordering, a
// partition, a pause, and a crash that keeps a random prefix of unsynced writes.
std::unique_ptr<Sim> chaos(std::uint64_t seed) {
    const std::vector<NodeId> ids{1, 2, 3, 4, 5};
    auto sim = std::make_unique<Sim>(seed, ids, [ids](NodeId me, raft::Env& env) {
        std::vector<NodeId> peers;
        std::ranges::copy_if(ids, std::back_inserter(peers), [me](NodeId p) { return p != me; });
        return std::make_unique<GossipNode>(me, peers, env);
    });
    Sim& s = *sim;
    s.faults().drop_rate = 0.1;
    s.faults().delay_min = 1ms;
    s.faults().delay_max = 20ms;
    s.faults().reorder = true;
    s.schedule(at(1s), "pause 2", [&s] { s.pause(2); });
    s.schedule(at(1500ms), "resume 2", [&s] { s.resume(2); });
    s.schedule(at(2s), "partition {1,2,3} {4,5}",
               [&s] { s.faults().partitions = {{1, 2, 3}, {4, 5}}; });
    s.schedule(at(3s), "crash 3",
               [&s] { s.crash(3, sim::SimStorage::CrashMode::KeepRandomPrefix); });
    s.schedule(at(4s), "heal", [&s] { s.faults().partitions.clear(); });
    s.schedule(at(5s), "restart 3", [&s] { s.restart(3); });
    s.start();
    s.run_until(at(10s));
    return sim;
}

// ---- determinism -------------------------------------------------------------------------

TEST(SimDeterminism, SameSeedGivesByteIdenticalLog) {
    for (std::uint64_t seed : seeds(20)) {
        SCOPED_TRACE(testing::Message() << "replay with RAFTKV_SEED=" << seed);
        auto a = chaos(seed);
        auto b = chaos(seed);
        ASSERT_GT(a->event_log().size(), 1000u);
        EXPECT_EQ(a->event_log(), b->event_log());
        EXPECT_EQ(a->log_hash(), b->log_hash());
    }
}

TEST(SimDeterminism, DifferentSeedsDiverge) {
    std::set<std::uint64_t> hashes;
    for (std::uint64_t seed = 1; seed <= 20; ++seed) hashes.insert(chaos(seed)->log_hash());
    EXPECT_EQ(hashes.size(), 20u);
}

// Golden values recorded on macOS (libc++). CI runs this on Linux (libstdc++) with gcc and
// clang, so a mismatch there means the simulation is not portable: look for a standard
// library distribution, unordered iteration, or anything else implementation-defined.
// Update these only for a deliberate change to the simulator or GossipNode.
TEST(SimDeterminism, GoldenLogMatchesAcrossPlatforms) {
    auto s = chaos(42);
    EXPECT_EQ(s->event_log().size(), 4765u);
    EXPECT_EQ(s->log_hash(), 12285342145284806052ULL);
}

// ---- Rng ---------------------------------------------------------------------------------

TEST(SimRng, MatchesReferenceSplitMix64) {
    sim::Rng r(0);
    EXPECT_EQ(r.next(), 0xe220a8397b1dcdafULL);
    EXPECT_EQ(r.next(), 0x6e789e6aa1b965f4ULL);
}

TEST(SimRng, BetweenStaysInRange) {
    sim::Rng r(7);
    for (int i = 0; i < 10'000; ++i) {
        const auto v = r.between(150, 300);
        ASSERT_GE(v, 150u);
        ASSERT_LE(v, 300u);
    }
    EXPECT_EQ(r.between(5, 5), 5u);
}

TEST(SimRng, ChanceEdges) {
    sim::Rng r(7);
    for (int i = 0; i < 1000; ++i) {
        ASSERT_FALSE(r.chance(0.0));
        ASSERT_TRUE(r.chance(1.0));
    }
}

// ---- network -----------------------------------------------------------------------------

struct Stream {
    std::vector<std::string> sent_ok;   // what the sim says it did not drop, in send order
    std::vector<std::string> received;
    sim::SimStats stats;
};

// Node 1 sends `n` numbered messages to node 2 at t=0.
Stream stream(std::uint64_t seed, int n, double drop, bool reorder) {
    Sim s(seed, {1, 2}, probes());
    s.faults().drop_rate = drop;
    s.faults().delay_min = 1ms;
    s.faults().delay_max = 50ms;
    s.faults().reorder = reorder;
    s.start();
    auto& env = s.node_as<ProbeNode>(1).env();
    Stream out;
    for (int i = 0; i < n; ++i) {
        const auto before = s.stats().dropped_loss;
        env.send({.to = 2, .method = "M", .payload = std::to_string(i)});
        if (s.stats().dropped_loss == before) out.sent_ok.push_back(std::to_string(i));
    }
    s.run_until(at(1s));
    out.received = s.node_as<ProbeNode>(2).messages;
    out.stats = s.stats();
    return out;
}

TEST(SimNetwork, LossyReorderingNetworkDeliversExactlyTheSurvivors) {
    for (std::uint64_t seed : seeds(10)) {
        SCOPED_TRACE(testing::Message() << "replay with RAFTKV_SEED=" << seed);
        const int n = 2000;
        Stream r = stream(seed, n, 0.3, /*reorder=*/true);

        // Every survivor arrives exactly once; nothing dropped arrives; nothing is invented.
        std::multiset<std::string> got(r.received.begin(), r.received.end());
        std::multiset<std::string> want(r.sent_ok.begin(), r.sent_ok.end());
        EXPECT_EQ(got, want);
        EXPECT_EQ(r.stats.delivered + r.stats.dropped_loss, static_cast<std::uint64_t>(n));

        // ~30% loss: 2000 draws put 4 standard deviations at about ±0.041.
        const double lost = static_cast<double>(r.stats.dropped_loss) / n;
        EXPECT_NEAR(lost, 0.3, 0.041);

        // Reordering actually happened.
        EXPECT_FALSE(std::ranges::is_sorted(r.received, {}, [](const std::string& x) {
            return std::stoi(x);
        }));
    }
}

TEST(SimNetwork, WithoutReorderEachLinkIsFifo) {
    Stream r = stream(3, 2000, 0.3, /*reorder=*/false);
    EXPECT_EQ(r.received, r.sent_ok);
}

TEST(SimNetwork, PartitionDropsAtSendAndInFlight) {
    Sim s(1, {1, 2, 3}, probes());
    s.faults().delay_min = s.faults().delay_max = 10ms;
    s.start();
    auto& env1 = s.node_as<ProbeNode>(1).env();

    s.faults().partitions = {{1, 2}, {3}};
    env1.send({.to = 3, .method = "M", .payload = "cut"});        // dropped at send
    env1.send({.to = 2, .method = "M", .payload = "same-side"});  // delivered
    s.faults().partitions.clear();
    env1.send({.to = 3, .method = "M", .payload = "in-flight"});  // partitioned while in flight
    s.faults().partitions = {{1, 2}, {3}};
    s.run_for(20ms);
    s.faults().partitions.clear();
    env1.send({.to = 3, .method = "M", .payload = "healed"});
    s.run_for(20ms);

    EXPECT_EQ(s.node_as<ProbeNode>(2).messages, std::vector<std::string>{"same-side"});
    EXPECT_EQ(s.node_as<ProbeNode>(3).messages, std::vector<std::string>{"healed"});
    EXPECT_EQ(s.stats().dropped_partition, 2u);
}

TEST(SimNetwork, UnlistedNodeIsIsolated) {
    Sim s(1, {1, 2, 3}, probes());
    s.start();
    s.faults().partitions = {{1, 2}};
    s.node_as<ProbeNode>(1).env().send({.to = 3, .method = "M", .payload = "x"});
    s.node_as<ProbeNode>(3).env().send({.to = 1, .method = "M", .payload = "y"});
    s.run_for(10ms);
    EXPECT_TRUE(s.node_as<ProbeNode>(3).messages.empty());
    EXPECT_TRUE(s.node_as<ProbeNode>(1).messages.empty());
}

TEST(SimNetwork, SenderCannotBeForged) {
    Sim s(1, {1, 2}, probes());
    s.start();
    s.node_as<ProbeNode>(1).env().send({.from = 99, .to = 2, .method = "M", .payload = "x"});
    s.run_for(10ms);
    ASSERT_FALSE(s.event_log().empty());
    EXPECT_NE(s.log_tail(1).find("deliver 1->2"), std::string::npos) << s.log_tail(3);
}

// ---- time and timers ---------------------------------------------------------------------

TEST(SimTime, TimeJumpsToEventsAndTiesRunInScheduleOrder) {
    Sim s(1, {1}, probes());
    std::vector<int> order;
    s.schedule(at(5s), "b", [&] { order.push_back(2); });
    s.schedule(at(1s), "a", [&] { order.push_back(1); });
    s.schedule(at(5s), "c", [&] { order.push_back(3); });
    s.run_until(at(5s));
    EXPECT_EQ(order, (std::vector<int>{1, 2, 3}));
    EXPECT_EQ(s.now(), at(5s));
    s.run_until(at(1h));   // an hour of virtual time with nothing to do costs nothing
    EXPECT_EQ(s.now(), at(1h));
}

TEST(SimTime, RunUntilPredicateStopsEarly) {
    Sim s(1, {1}, probes());
    int ticks = 0;
    for (int i = 1; i <= 10; ++i) s.schedule(at(i * 1s), "tick", [&] { ++ticks; });
    EXPECT_TRUE(s.run_until([&] { return ticks == 3; }, at(1min)));
    EXPECT_EQ(s.now(), at(3s));
    EXPECT_FALSE(s.run_until([&] { return ticks == 99; }, at(20s)));
    EXPECT_EQ(s.now(), at(20s));
}

TEST(SimTimers, FireInOrderAndCancelWorks) {
    Sim s(1, {1}, probes());
    s.start();
    auto& n = s.node_as<ProbeNode>(1);
    n.env().after(30ms, 3);
    const auto cancelled = n.env().after(20ms, 2);
    n.env().after(10ms, 1);
    n.env().cancel(cancelled);
    n.env().cancel(cancelled);   // no-op
    s.run_for(1s);
    EXPECT_EQ(n.timers, (std::vector<raft::TimerTag>{1, 3}));
}

TEST(SimTimers, CrashDropsPendingTimers) {
    Sim s(1, {1}, probes());
    s.start();
    s.node_as<ProbeNode>(1).env().after(100ms, 7);
    s.crash(1);
    s.restart(1);
    s.run_for(1s);
    EXPECT_TRUE(s.node_as<ProbeNode>(1).timers.empty());
    EXPECT_EQ(s.node_as<ProbeNode>(1).starts, 1);
}

// ---- node faults -------------------------------------------------------------------------

TEST(SimFaults, PausedNodeHandlesEverythingOnResumeInArrivalOrder) {
    Sim s(1, {1, 2}, probes());
    s.faults().delay_min = s.faults().delay_max = 1ms;
    s.start();
    s.pause(2);
    s.node_as<ProbeNode>(2).env().after(5ms, 42);
    s.node_as<ProbeNode>(1).env().send({.to = 2, .method = "M", .payload = "a"});
    s.run_for(10ms);
    s.node_as<ProbeNode>(1).env().send({.to = 2, .method = "M", .payload = "b"});
    s.run_for(10ms);
    EXPECT_TRUE(s.node_as<ProbeNode>(2).messages.empty());
    EXPECT_TRUE(s.node_as<ProbeNode>(2).timers.empty());

    s.resume(2);
    s.run_for(1ms);
    EXPECT_EQ(s.node_as<ProbeNode>(2).messages, (std::vector<std::string>{"a", "b"}));
    EXPECT_EQ(s.node_as<ProbeNode>(2).timers, (std::vector<raft::TimerTag>{42}));
}

TEST(SimFaults, CrashAfterResumeDoesNotDeliverToDeadNode) {
    Sim s(1, {1, 2}, probes());
    s.start();
    s.pause(2);
    s.node_as<ProbeNode>(1).env().send({.to = 2, .method = "M", .payload = "a"});
    s.run_for(10ms);
    s.resume(2);   // "a" is queued for now...
    s.crash(2);    // ...but the node dies first
    s.run_for(10ms);
    EXPECT_FALSE(s.is_up(2));
    EXPECT_EQ(s.stats().dropped_down, 1u);
}

TEST(SimFaults, MessagesToADownNodeAreDropped) {
    Sim s(1, {1, 2}, probes());
    s.start();
    s.crash(2);
    s.node_as<ProbeNode>(1).env().send({.to = 2, .method = "M", .payload = "lost"});
    s.run_for(10ms);
    s.restart(2);
    s.run_for(10ms);
    EXPECT_TRUE(s.node_as<ProbeNode>(2).messages.empty());
    EXPECT_EQ(s.stats().dropped_down, 1u);
}

TEST(SimFaults, MisuseThrows) {
    Sim s(1, {1}, probes());
    s.start();
    EXPECT_THROW(s.restart(1), std::logic_error);
    EXPECT_THROW(s.node(9), std::logic_error);
    s.crash(1);
    EXPECT_THROW(s.crash(1), std::logic_error);
    EXPECT_THROW(s.node_as<ProbeNode>(1), std::logic_error);
    EXPECT_THROW(Sim(1, {1, 1}, probes()), std::logic_error);
}

// ---- storage through the simulator -------------------------------------------------------

TEST(SimCrash, RestartedNodeSeesExactlyTheSyncedWrites) {
    for (std::uint64_t seed : seeds(20)) {
        SCOPED_TRACE(testing::Message() << "replay with RAFTKV_SEED=" << seed);
        auto s = chaos(seed);
        // Crash every node; each must come back with exactly what it had synced.
        for (NodeId id = 1; id <= 5; ++id) {
            const raft::PersistentState synced = s->storage(id).synced();
            s->crash(id);
            EXPECT_EQ(s->storage(id).load(), synced);
            s->restart(id);
            EXPECT_EQ(s->storage(id).load(), synced);
        }
    }
}

}  // namespace
}  // namespace raftkv
