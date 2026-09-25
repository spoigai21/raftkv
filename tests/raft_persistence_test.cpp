#include <gtest/gtest.h>

#include <fstream>
#include <iterator>

#include "raft_cluster.hpp"
#include "temp_dir.hpp"
#include "test_seeds.hpp"

namespace raftkv {
namespace {

using namespace std::chrono_literals;
using raft::NodeId;
using test::RaftCluster;
using test::seeds;
using test::TempDir;

#define EXPECT_SAFE(c) EXPECT_TRUE((c).violations().empty()) \
    << testing::PrintToString((c).violations()) << "\n" << (c).context()

void edit_file(const std::filesystem::path& p, const std::function<void(std::vector<char>&)>& fn) {
    std::vector<char> bytes;
    {
        std::ifstream in(p, std::ios::binary);
        bytes.assign(std::istreambuf_iterator<char>(in), {});
    }
    fn(bytes);
    std::ofstream out(p, std::ios::binary | std::ios::trunc);
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
}

// The Phase 4 done-gate, run in the simulator on real files: every node is killed at once,
// again and again, in the middle of a workload. Each restart rebuilds the node from its data
// directory, and the invariant checker confirms after every event that nothing committed
// ever changes or disappears.
TEST(RaftPersistence, KillingEveryNodeMidWorkloadKeepsTheCommittedPrefix) {
    for (std::uint64_t seed : seeds(10)) {
        TempDir dir;
        RaftCluster c(seed, 3);
        c.use_file_storage(dir.path());
        c.schedule_workload(10ms, raft::Time{6s});
        for (int k = 1; k <= 5; ++k) {
            const raft::Time at{k * 1s};
            c.sim().schedule(at, "kill every node", [&c] {
                for (NodeId id : c.ids()) if (c.sim().is_up(id)) c.sim().crash(id);
            });
            c.sim().schedule(at + 50ms, "restart every node", [&c] {
                for (NodeId id : c.ids()) c.sim().restart(id);
            });
        }
        c.sim().start();
        c.sim().run_until(raft::Time{6s});
        ASSERT_TRUE(c.violations().empty()) << testing::PrintToString(c.violations()) << c.context();

        ASSERT_TRUE(c.wait_for_leader(2s)) << c.context();
        c.propose("final");
        ASSERT_TRUE(c.wait_for_convergence(3s)) << c.context();
        EXPECT_GT(c.max_committed(), 100u) << "the workload made progress between kills";
        EXPECT_SAFE(c);
    }
}

TEST(RaftPersistence, TornTailIsCutAndTheNodeCatchesUp) {
    TempDir dir;
    RaftCluster c(4, 3);
    c.use_file_storage(dir.path());
    c.sim().start();
    ASSERT_TRUE(c.wait_for_leader(2s));
    for (int i = 0; i < 20; ++i) c.propose("c" + std::to_string(i));
    ASSERT_TRUE(c.wait_for_convergence(2s));

    const NodeId victim = *c.leader() == 1 ? 2 : 1;
    c.sim().crash(victim);
    // Half of a record that was being written when the process died.
    edit_file(dir / std::format("node-{}/log", victim),
              [](std::vector<char>& b) { b.insert(b.end(), {'\x09', '\x00', '\x00'}); });
    c.sim().restart(victim);
    ASSERT_TRUE(c.sim().is_up(victim)) << c.sim().boot_error(victim);
    ASSERT_TRUE(c.wait_for_convergence(2s)) << c.context();
    EXPECT_EQ(c.applied(victim).size(), 20u);
    EXPECT_SAFE(c);
}

// A node whose synced log is damaged must not come back and serve a shortened history.
TEST(RaftPersistence, CorruptLogMakesTheNodeRefuseToStart) {
    TempDir dir;
    RaftCluster c(5, 3);
    c.use_file_storage(dir.path());
    c.sim().start();
    ASSERT_TRUE(c.wait_for_leader(2s));
    for (int i = 0; i < 20; ++i) c.propose("c" + std::to_string(i));
    ASSERT_TRUE(c.wait_for_convergence(2s));

    const NodeId victim = *c.leader() == 1 ? 2 : 1;
    c.sim().crash(victim);
    edit_file(dir / std::format("node-{}/log", victim),
              [](std::vector<char>& b) { b[b.size() / 2] ^= 0x04; });
    c.sim().restart(victim);
    EXPECT_FALSE(c.sim().is_up(victim));
    EXPECT_NE(c.sim().boot_error(victim).find("corrupt"), std::string::npos)
        << c.sim().boot_error(victim);

    // The other two are a majority and keep committing without it.
    const raft::Index before = c.max_committed();
    for (int i = 0; i < 5; ++i) c.propose("more" + std::to_string(i));
    c.sim().run_for(1s);
    EXPECT_GE(c.max_committed(), before + 5);
    EXPECT_SAFE(c);
}

TEST(RaftPersistence, ChaosOnRealFiles) {
    for (std::uint64_t seed : seeds(20)) {
        TempDir dir;
        RaftCluster c(seed, 3);
        c.quiet();
        c.use_file_storage(dir.path());
        c.schedule_chaos(seed, 5s);
        c.schedule_workload(20ms, raft::Time{5s});
        c.sim().start();
        c.sim().run_until(raft::Time{5s});
        ASSERT_TRUE(c.violations().empty()) << testing::PrintToString(c.violations()) << c.context();
        ASSERT_TRUE(c.wait_for_leader(3s)) << c.context();
        c.propose("final");
        ASSERT_TRUE(c.wait_for_convergence(5s)) << c.context();
        EXPECT_SAFE(c);
    }
}

}  // namespace
}  // namespace raftkv
