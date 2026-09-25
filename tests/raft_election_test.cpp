#include <gtest/gtest.h>

#include <set>

#include "raft/messages.hpp"
#include "raft/raft.hpp"
#include "raft_cluster.hpp"
#include "sim_test_nodes.hpp"
#include "test_seeds.hpp"

namespace raftkv {
namespace {

using namespace std::chrono_literals;
using raft::NodeId;
using raft::Role;
using test::RaftCluster;
using test::seeds;

#define EXPECT_SAFE(c) EXPECT_TRUE((c).violations().empty()) \
    << testing::PrintToString((c).violations()) << "\n" << (c).context()

// ---- the Phase 2 done-gate ---------------------------------------------------------------

TEST(RaftElection, ThreeNodesElectExactlyOneLeader) {
    for (std::uint64_t seed : seeds(50)) {
        RaftCluster c(seed, 3);
        c.sim().start();
        ASSERT_TRUE(c.wait_for_leader(2s)) << c.context();
        const NodeId leader = *c.leader();
        c.sim().run_for(1s);   // let heartbeats reach everyone

        EXPECT_EQ(c.leader(), leader) << c.context();
        EXPECT_TRUE(c.all_follow(leader)) << c.context();
        int leaders = 0;
        for (NodeId id : c.ids()) leaders += c.raft(id)->role() == Role::Leader;
        EXPECT_EQ(leaders, 1) << c.context();
        EXPECT_SAFE(c);
    }
}

TEST(RaftElection, KillingTheLeaderElectsANewOne) {
    for (std::uint64_t seed : seeds(50)) {
        RaftCluster c(seed, 3);
        c.sim().start();
        ASSERT_TRUE(c.wait_for_leader(2s)) << c.context();
        const NodeId old = *c.leader();
        const raft::Term old_term = c.raft(old)->current_term();

        c.sim().crash(old);
        ASSERT_TRUE(c.wait_for_leader(2s)) << c.context();
        EXPECT_NE(*c.leader(), old);
        EXPECT_GT(c.raft(*c.leader())->current_term(), old_term);

        c.sim().restart(old);   // comes back as a follower of the new leader
        c.sim().run_for(1s);
        EXPECT_EQ(c.raft(old)->role(), Role::Follower) << c.context();
        EXPECT_TRUE(c.all_follow(*c.leader())) << c.context();
        EXPECT_SAFE(c);
    }
}

TEST(RaftElection, OldLeaderStepsDownWhenPartitionHeals) {
    for (std::uint64_t seed : seeds(50)) {
        RaftCluster c(seed, 3);
        c.sim().start();
        ASSERT_TRUE(c.wait_for_leader(2s)) << c.context();
        const NodeId old = *c.leader();

        std::vector<NodeId> rest;
        for (NodeId id : c.ids()) if (id != old) rest.push_back(id);
        c.sim().faults().partitions = {{old}, rest};
        c.sim().run_for(2s);

        // The majority side has moved on; the old leader, alone, still believes it leads.
        ASSERT_TRUE(c.leader().has_value()) << c.context();
        EXPECT_NE(*c.leader(), old);
        EXPECT_EQ(c.raft(old)->role(), Role::Leader);
        EXPECT_LT(c.raft(old)->current_term(), c.raft(*c.leader())->current_term());

        c.sim().faults().partitions.clear();
        c.sim().run_for(1s);
        EXPECT_EQ(c.raft(old)->role(), Role::Follower) << c.context();
        EXPECT_TRUE(c.all_follow(*c.leader())) << c.context();
        EXPECT_SAFE(c);
    }
}

TEST(RaftElection, SameSeedReplaysIdentically) {
    auto run = [](std::uint64_t seed) {
        RaftCluster c(seed, 5);
        c.schedule_chaos(seed, 5s);
        c.sim().start();
        c.sim().run_until(raft::Time{8s});
        return std::pair(c.sim().log_hash(), c.sim().event_log().size());
    };
    for (std::uint64_t seed : seeds(5)) EXPECT_EQ(run(seed), run(seed)) << "seed " << seed;
}

// Recorded on macOS; CI checks it on Linux with gcc and clang. Covers what the simulator's
// own golden test does not: Raft's use of randomness and timers, and protobuf encoding sizes.
TEST(RaftElection, GoldenChaosRunMatchesAcrossPlatforms) {
    RaftCluster c(42, 5);
    c.schedule_chaos(42, 5s);
    c.sim().start();
    c.sim().run_until(raft::Time{8s});
    EXPECT_EQ(c.sim().event_log().size(), 2660u);
    EXPECT_EQ(c.sim().log_hash(), 8155514256532082736ULL);
}

// ---- beyond the gate ---------------------------------------------------------------------

TEST(RaftElection, HealthyClusterStaysStable) {
    RaftCluster c(7, 5);
    c.sim().start();
    ASSERT_TRUE(c.wait_for_leader(2s));
    const NodeId leader = *c.leader();
    const raft::Term term = c.raft(leader)->current_term();
    c.sim().run_for(10s);
    EXPECT_EQ(c.leader(), leader);
    for (NodeId id : c.ids()) EXPECT_EQ(c.raft(id)->current_term(), term) << "node " << id;
    EXPECT_SAFE(c);
}

TEST(RaftElection, NoQuorumMeansNoLeader) {
    RaftCluster c(3, 3);
    c.sim().start();
    c.sim().crash(2);
    c.sim().crash(3);
    c.sim().run_for(5s);
    EXPECT_EQ(c.raft(1)->role(), Role::Candidate);
    EXPECT_GT(c.raft(1)->current_term(), 5u);   // it keeps trying, with rising terms
    c.sim().restart(2);
    EXPECT_TRUE(c.wait_for_leader(2s)) << c.context();
    EXPECT_SAFE(c);
}

TEST(RaftElection, FiveNodesSurviveTwoFailures) {
    for (std::uint64_t seed : seeds(20)) {
        RaftCluster c(seed, 5);
        c.sim().start();
        ASSERT_TRUE(c.wait_for_leader(2s)) << c.context();
        const NodeId first = *c.leader();
        c.sim().crash(first);
        c.sim().crash(first == 1 ? 2 : 1);
        EXPECT_TRUE(c.wait_for_leader(3s)) << c.context();
        EXPECT_SAFE(c);
    }
}

TEST(RaftElection, PausedLeaderIsReplacedAndStepsDownOnResume) {
    for (std::uint64_t seed : seeds(20)) {
        RaftCluster c(seed, 3);
        c.sim().start();
        ASSERT_TRUE(c.wait_for_leader(2s)) << c.context();
        const NodeId old = *c.leader();
        c.sim().pause(old);
        ASSERT_TRUE(c.wait_for_leader(2s)) << c.context();
        EXPECT_NE(*c.leader(), old);
        c.sim().resume(old);
        c.sim().run_for(1s);
        EXPECT_EQ(c.raft(old)->role(), Role::Follower) << c.context();
        EXPECT_SAFE(c);
    }
}

TEST(RaftElection, SingleNodeClusterElectsItself) {
    RaftCluster c(1, 1);
    c.sim().start();
    EXPECT_TRUE(c.wait_for_leader(1s));
    EXPECT_EQ(c.leader(), 1u);
}

// Random crashes, restarts, partitions, pauses and loss. Election Safety is checked after
// every event; once everything heals, a leader must appear.
TEST(RaftElection, ChaosKeepsElectionSafetyAndRecovers) {
    for (std::uint64_t seed : seeds(200)) {
        for (int n : {3, 5}) {
            RaftCluster c(seed, n);
            c.sim().keep_log_lines(false);   // hash only; rerun one seed to see the log
            c.schedule_chaos(seed, 10s);
            c.sim().start();
            c.sim().run_until(raft::Time{10s});
            ASSERT_TRUE(c.violations().empty())
                << "n=" << n << " " << testing::PrintToString(c.violations()) << "\n" << c.context();
            EXPECT_TRUE(c.wait_for_leader(3s)) << "no leader after healing, n=" << n << "\n"
                                               << c.context();
        }
    }
}

// ---- single-node checks, with probes as peers ---------------------------------------------

// Node 1 is Raft; nodes 2 and 3 are probes that send hand-made RPCs and record replies.
struct ProbeRig {
    explicit ProbeRig(std::vector<raft::LogEntry> preload = {})
        : sim(1, {1, 2, 3}, [](NodeId id, raft::Env& env) -> std::unique_ptr<raft::Node> {
              if (id == 1) return std::make_unique<raft::Raft>(raft::RaftConfig{.id = 1, .peers = {2, 3}}, env);
              return std::make_unique<test::ProbeNode>(env);
          }) {
        sim.describe_messages_with(raft::describe);
        if (!preload.empty()) {
            sim.storage(1).append(preload);
            sim.storage(1).sync();
        }
        sim.start();
    }

    // Sends from probe `from` to node 1, runs 10ms (well inside the 150ms election timeout),
    // and returns the last reply the probe received.
    raft::Rpc ask(NodeId from, const raft::Rpc& rpc) {
        auto& probe = sim.node_as<test::ProbeNode>(from);
        probe.env().send(raft::encode(1, rpc));
        sim.run_for(10ms);
        EXPECT_FALSE(probe.inbox.empty()) << sim.log_tail(10);
        auto reply = raft::parse(probe.inbox.back());
        EXPECT_TRUE(reply.has_value());
        return *reply;
    }

    bool vote(NodeId from, raft::Term term, raft::Index last_index = 0, raft::Term last_term = 0) {
        auto r = ask(from, raft::RequestVote{.term = term, .candidate_id = from,
                                             .last_log_index = last_index, .last_log_term = last_term});
        return std::get<raft::RequestVoteReply>(r).vote_granted;
    }

    raft::Raft& node() { return sim.node_as<raft::Raft>(1); }

    sim::Sim sim;
};

TEST(RaftVoting, OneVotePerTerm) {
    ProbeRig rig;
    EXPECT_TRUE(rig.vote(2, 1));
    EXPECT_FALSE(rig.vote(3, 1));   // already voted for 2 in term 1
    EXPECT_TRUE(rig.vote(2, 1));    // asking again gets the same answer
    EXPECT_TRUE(rig.vote(3, 2));    // a new term, a new vote
}

TEST(RaftVoting, VoteSurvivesACrash) {
    ProbeRig rig;
    EXPECT_TRUE(rig.vote(2, 1));
    rig.sim.crash(1);
    rig.sim.restart(1);
    EXPECT_EQ(rig.node().voted_for(), 2u);
    EXPECT_FALSE(rig.vote(3, 1));
}

TEST(RaftVoting, RefusesCandidatesWithStaleLogs) {
    // Node 1 has entries 1..3 from term 2.
    ProbeRig rig({{2, 1, "a"}, {2, 2, "b"}, {2, 3, "c"}});
    EXPECT_FALSE(rig.vote(2, 5, 9, 1));   // longer log, but an older last term
    EXPECT_FALSE(rig.vote(2, 5, 2, 2));   // same last term, shorter
    EXPECT_EQ(rig.node().current_term(), 5u);   // the refusal still adopts the newer term
    EXPECT_TRUE(rig.vote(3, 5, 3, 2));    // same last term, same length: up to date
}

TEST(RaftVoting, AcceptsNewerLastTermEvenIfShorter) {
    ProbeRig rig({{1, 1, "a"}, {1, 2, "b"}, {1, 3, "c"}});
    EXPECT_TRUE(rig.vote(2, 4, 1, 2));
}

TEST(RaftVoting, RejectsForgedCandidateId) {
    ProbeRig rig;
    auto& probe = rig.sim.node_as<test::ProbeNode>(2);
    probe.env().send(raft::encode(1, raft::RequestVote{.term = 1, .candidate_id = 3,
                                                        .last_log_index = 0, .last_log_term = 0}));
    rig.sim.run_for(10ms);
    EXPECT_TRUE(probe.inbox.empty());
    EXPECT_FALSE(rig.node().voted_for().has_value());
}

TEST(RaftAppendEntries, StaleTermIsRejectedWithCurrentTerm) {
    ProbeRig rig;
    rig.vote(2, 5);
    auto r = rig.ask(3, raft::AppendEntries{.term = 3, .leader_id = 3, .prev_log_index = 0,
                                            .prev_log_term = 0, .entries = {}, .leader_commit = 0});
    const auto& reply = std::get<raft::AppendEntriesReply>(r);
    EXPECT_FALSE(reply.success);
    EXPECT_EQ(reply.term, 5u);
}

TEST(RaftAppendEntries, CurrentLeaderIsRecognized) {
    ProbeRig rig;
    auto r = rig.ask(3, raft::AppendEntries{.term = 2, .leader_id = 3, .prev_log_index = 0,
                                            .prev_log_term = 0, .entries = {}, .leader_commit = 0});
    EXPECT_TRUE(std::get<raft::AppendEntriesReply>(r).success);
    EXPECT_EQ(rig.node().leader(), 3u);
    EXPECT_EQ(rig.node().current_term(), 2u);
}

TEST(RaftMessages, MalformedMessageIsDroppedNotFatal) {
    ProbeRig rig;
    rig.sim.node_as<test::ProbeNode>(2).env().send(
        {.from = 2, .to = 1, .method = "RequestVote", .payload = "\xff\xff\xff"});
    rig.sim.node_as<test::ProbeNode>(2).env().send(
        {.from = 2, .to = 1, .method = "Nonsense", .payload = ""});
    rig.sim.run_for(10ms);
    EXPECT_NE(rig.sim.log_tail(5).find("dropped message"), std::string::npos) << rig.sim.log_tail(5);
    EXPECT_EQ(rig.node().current_term(), 0u);
}

TEST(RaftMessages, RoundTrip) {
    const raft::AppendEntries ae{.term = 7, .leader_id = 2, .prev_log_index = 4, .prev_log_term = 6,
                                 .entries = {{6, 5, "x"}, {7, 6, std::string("\0y", 2)}},
                                 .leader_commit = 3};
    auto back = raft::parse(raft::encode(1, ae));
    ASSERT_TRUE(back.has_value());
    const auto& got = std::get<raft::AppendEntries>(*back);
    EXPECT_EQ(got.term, 7u);
    EXPECT_EQ(got.leader_id, 2u);
    EXPECT_EQ(got.prev_log_index, 4u);
    EXPECT_EQ(got.prev_log_term, 6u);
    EXPECT_EQ(got.entries, ae.entries);
    EXPECT_EQ(got.leader_commit, 3u);
}

}  // namespace
}  // namespace raftkv
