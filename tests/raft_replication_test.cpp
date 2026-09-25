#include <gtest/gtest.h>

#include "raft/messages.hpp"
#include "raft/raft.hpp"
#include "raft_cluster.hpp"
#include "sim_test_nodes.hpp"
#include "test_seeds.hpp"

namespace raftkv {
namespace {

using namespace std::chrono_literals;
using raft::LogEntry;
using raft::NodeId;
using raft::Role;
using test::RaftCluster;
using test::seeds;

#define EXPECT_SAFE(c) EXPECT_TRUE((c).violations().empty()) \
    << testing::PrintToString((c).violations()) << "\n" << (c).context()

std::vector<std::string> numbered(const std::string& prefix, int n) {
    std::vector<std::string> out;
    for (int i = 0; i < n; ++i) out.push_back(prefix + std::to_string(i));
    return out;
}

// ---- the Phase 3 done-gate ---------------------------------------------------------------

TEST(RaftReplication, HundredEntriesAppearInIdenticalOrderEverywhere) {
    for (std::uint64_t seed : seeds(20)) {
        RaftCluster c(seed, 3);
        c.sim().start();
        ASSERT_TRUE(c.wait_for_leader(2s)) << c.context();
        for (const std::string& cmd : numbered("c", 100)) ASSERT_TRUE(c.propose(cmd)->accepted);

        ASSERT_TRUE(c.wait_for_convergence(2s)) << c.context();
        for (NodeId id : c.ids()) EXPECT_EQ(c.applied(id), numbered("c", 100)) << "node " << id;
        EXPECT_SAFE(c);
    }
}

TEST(RaftReplication, PartitionedFollowerCatchesUpOnRejoin) {
    for (std::uint64_t seed : seeds(20)) {
        RaftCluster c(seed, 3);
        c.sim().start();
        ASSERT_TRUE(c.wait_for_leader(2s)) << c.context();
        const NodeId leader = *c.leader();
        const NodeId cut = leader == 3 ? 2 : 3;
        std::vector<NodeId> rest;
        for (NodeId id : c.ids()) if (id != cut) rest.push_back(id);
        c.sim().faults().partitions = {{cut}, rest};

        for (const std::string& cmd : numbered("c", 50)) ASSERT_TRUE(c.propose(cmd)->accepted);
        c.sim().run_for(1s);
        // The majority committed all 50 without the cut-off follower.
        EXPECT_EQ(c.applied(leader), numbered("c", 50)) << c.context();
        EXPECT_TRUE(c.applied(cut).empty());

        c.sim().faults().partitions.clear();
        ASSERT_TRUE(c.wait_for_convergence(3s)) << c.context();
        EXPECT_EQ(c.applied(cut), numbered("c", 50)) << c.context();
        EXPECT_SAFE(c);
    }
}

// ---- beyond the gate ---------------------------------------------------------------------

TEST(RaftReplication, ProposalToAFollowerIsRejectedWithAHint) {
    RaftCluster c(5, 3);
    c.sim().start();
    ASSERT_TRUE(c.wait_for_leader(2s));
    c.sim().run_for(200ms);
    const NodeId leader = *c.leader();
    const NodeId follower = leader == 1 ? 2 : 1;
    const raft::ProposeResult r = c.raft(follower)->propose("x");
    EXPECT_FALSE(r.accepted);
    EXPECT_EQ(r.leader_hint, leader);
}

TEST(RaftReplication, SingleNodeClusterCommitsAlone) {
    RaftCluster c(1, 1);
    c.sim().start();
    ASSERT_TRUE(c.wait_for_leader(1s));
    for (const std::string& cmd : numbered("c", 10)) c.propose(cmd);
    ASSERT_TRUE(c.wait_for_convergence(100ms));
    EXPECT_EQ(c.applied(1), numbered("c", 10));
}

TEST(RaftReplication, DivergentFollowerLogIsOverwritten) {
    // Node 3 holds entries 2..4 from term 1 that were never committed; nodes 1 and 2 hold a
    // different entry 2 from term 2. Node 3 cannot win an election (its last term is older),
    // so whoever leads must replace node 3's suffix.
    RaftCluster c(3, 3);
    for (NodeId id : {1, 2}) {
        c.sim().storage(id).save_hard_state(2, std::nullopt);
        c.sim().storage(id).append(std::vector<LogEntry>{{1, 1, "a"}, {2, 2, "b"}});
        c.sim().storage(id).sync();
    }
    c.sim().storage(3).save_hard_state(1, std::nullopt);
    c.sim().storage(3).append(std::vector<LogEntry>{{1, 1, "a"}, {1, 2, "x"}, {1, 3, "y"}, {1, 4, "z"}});
    c.sim().storage(3).sync();

    c.sim().start();
    ASSERT_TRUE(c.wait_for_leader(2s)) << c.context();
    EXPECT_NE(*c.leader(), 3u);
    ASSERT_TRUE(c.wait_for_convergence(2s)) << c.context();
    EXPECT_EQ(c.applied(3), (std::vector<std::string>{"a", "b"}));
    EXPECT_NE(c.sim().log_tail(10000).find("node 3: truncate log from 2"), std::string::npos);
    EXPECT_SAFE(c);
}

TEST(RaftReplication, RestartedNodeReappliesFromTheStart) {
    RaftCluster c(9, 3);
    c.sim().start();
    ASSERT_TRUE(c.wait_for_leader(2s));
    for (const std::string& cmd : numbered("c", 20)) c.propose(cmd);
    ASSERT_TRUE(c.wait_for_convergence(2s));
    const NodeId follower = *c.leader() == 1 ? 2 : 1;
    c.sim().crash(follower);
    c.sim().restart(follower);
    ASSERT_TRUE(c.wait_for_convergence(2s)) << c.context();
    EXPECT_EQ(c.applied(follower), numbered("c", 20));
    EXPECT_SAFE(c);
}

TEST(RaftReplication, EntriesSurviveAFullClusterCrash) {
    RaftCluster c(11, 3);
    c.sim().start();
    ASSERT_TRUE(c.wait_for_leader(2s));
    for (const std::string& cmd : numbered("c", 30)) c.propose(cmd);
    ASSERT_TRUE(c.wait_for_convergence(2s));
    for (NodeId id : c.ids()) c.sim().crash(id);
    for (NodeId id : c.ids()) c.sim().restart(id);
    ASSERT_TRUE(c.wait_for_leader(2s)) << c.context();
    ASSERT_TRUE(c.wait_for_convergence(2s)) << c.context();
    for (NodeId id : c.ids()) EXPECT_EQ(c.applied(id), numbered("c", 30)) << "node " << id;
    EXPECT_SAFE(c);
}

// Random faults under a steady client workload. Every invariant is checked after every event;
// once everything heals, all nodes must converge on one log that keeps every entry that was
// ever committed.
TEST(RaftReplication, ChaosWithWorkloadKeepsEveryInvariantAndConverges) {
    for (std::uint64_t seed : seeds(100)) {
        for (int n : {3, 5}) {
            RaftCluster c(seed, n);
            c.quiet();
            c.schedule_chaos(seed, 10s);
            c.schedule_workload(20ms, raft::Time{10s});
            c.sim().start();
            c.sim().run_until(raft::Time{10s});
            ASSERT_TRUE(c.violations().empty())
                << "n=" << n << " " << testing::PrintToString(c.violations()) << "\n" << c.context();

            ASSERT_TRUE(c.wait_for_leader(3s)) << "n=" << n << "\n" << c.context();
            c.propose("final");   // commits everything before it, from the new term
            ASSERT_TRUE(c.wait_for_convergence(5s)) << "n=" << n << "\n" << c.context();
            EXPECT_TRUE(c.violations().empty()) << testing::PrintToString(c.violations());
            EXPECT_GT(c.max_committed(), 10u) << "the workload made progress";
        }
    }
}

// ---- the commit rule, driven by hand (§5.4.2, Figure 8) -----------------------------------

// Node 1 is Raft; nodes 2 and 3 are probes that play the other servers.
struct LeaderRig {
    LeaderRig()
        : sim(1, {1, 2, 3}, [](NodeId id, raft::Env& env) -> std::unique_ptr<raft::Node> {
              if (id == 1) return std::make_unique<raft::Raft>(raft::RaftConfig{.id = 1, .peers = {2, 3}}, env);
              return std::make_unique<test::ProbeNode>(env);
          }) {
        sim.describe_messages_with(raft::describe);
    }

    void reply(NodeId from, const raft::Rpc& rpc) {
        sim.node_as<test::ProbeNode>(from).env().send(raft::encode(1, rpc));
        sim.run_for(5ms);
    }

    raft::Raft& node() { return sim.node_as<raft::Raft>(1); }
    sim::Sim sim;
};

TEST(RaftCommitRule, OldTermEntryIsNotCommittedByCountingReplicas) {
    LeaderRig rig;
    // Node 1 holds entry 1 (term 1) and entry 2 (term 2), neither known to be committed.
    rig.sim.storage(1).save_hard_state(2, std::nullopt);
    rig.sim.storage(1).append(std::vector<LogEntry>{{1, 1, "a"}, {2, 2, "b"}});
    rig.sim.storage(1).sync();
    rig.sim.start();

    // Let it time out and stand for election; probe 2 votes for it.
    auto& probe2 = rig.sim.node_as<test::ProbeNode>(2);
    ASSERT_TRUE(rig.sim.run_until([&] { return !probe2.inbox.empty(); }, raft::Time{1s}));
    const raft::Term term = rig.node().current_term();
    ASSERT_EQ(term, 3u);
    rig.reply(2, raft::RequestVoteReply{.term = term, .vote_granted = true});
    ASSERT_EQ(rig.node().role(), Role::Leader);
    ASSERT_EQ(rig.node().last_log_index(), 3u);   // its term-3 no-op

    // Probe 2 now stores entries 1..2. With node 1 that is a majority for entry 2, but entry 2
    // is from term 2, so counting replicas must not commit it.
    rig.reply(2, raft::AppendEntriesReply{.term = term, .success = true, .match_index = 2});
    EXPECT_EQ(rig.node().commit_index(), 0u) << rig.sim.log_tail(20);

    // Once a majority stores the term-3 entry, it and everything before it commit.
    rig.reply(2, raft::AppendEntriesReply{.term = term, .success = true, .match_index = 3});
    EXPECT_EQ(rig.node().commit_index(), 3u) << rig.sim.log_tail(20);
}

TEST(RaftCommitRule, StaleReplyCannotMoveMatchBackwards) {
    LeaderRig rig;
    rig.sim.start();
    auto& probe2 = rig.sim.node_as<test::ProbeNode>(2);
    ASSERT_TRUE(rig.sim.run_until([&] { return !probe2.inbox.empty(); }, raft::Time{1s}));
    const raft::Term term = rig.node().current_term();
    rig.reply(2, raft::RequestVoteReply{.term = term, .vote_granted = true});
    ASSERT_EQ(rig.node().role(), Role::Leader);
    for (int i = 0; i < 3; ++i) rig.node().propose("c");   // log: no-op, c, c, c

    rig.reply(2, raft::AppendEntriesReply{.term = term, .success = true, .match_index = 4});
    EXPECT_EQ(rig.node().commit_index(), 4u);
    // A delayed failure and a delayed older success arrive afterwards: nothing regresses.
    rig.reply(2, raft::AppendEntriesReply{.term = term, .success = false, .match_index = 0,
                                          .conflict_index = 1, .conflict_term = 0});
    rig.reply(2, raft::AppendEntriesReply{.term = term, .success = true, .match_index = 1});
    rig.node().propose("d");
    EXPECT_EQ(rig.node().commit_index(), 4u);
    rig.reply(2, raft::AppendEntriesReply{.term = term, .success = true, .match_index = 5});
    EXPECT_EQ(rig.node().commit_index(), 5u);
}

}  // namespace
}  // namespace raftkv
