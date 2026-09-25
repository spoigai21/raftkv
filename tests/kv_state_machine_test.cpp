#include "kv/state_machine.hpp"

#include <gtest/gtest.h>

namespace raftkv {
namespace {

using kv::Command;
using kv::Op;
using kv::Result;
using kv::StateMachine;

Command cmd(std::uint64_t client, std::uint64_t seq, Op op, std::string key, std::string value = {}) {
    return {.client_id = client, .seq = seq, .op = op, .key = std::move(key), .value = std::move(value)};
}

TEST(KvStateMachine, PutGetAppend) {
    StateMachine sm;
    EXPECT_EQ(sm.apply(cmd(1, 1, Op::Get, "k")), (Result{.value = "", .found = false}));
    sm.apply(cmd(1, 2, Op::Put, "k", "a"));
    sm.apply(cmd(1, 3, Op::Append, "k", "b"));
    sm.apply(cmd(1, 4, Op::Append, "new", "x"));   // append to a missing key creates it
    EXPECT_EQ(sm.apply(cmd(1, 5, Op::Get, "k")), (Result{.value = "ab", .found = true}));
    EXPECT_EQ(sm.get("new"), "x");
    sm.apply(cmd(1, 6, Op::Put, "k", "z"));
    EXPECT_EQ(sm.get("k"), "z");
}

TEST(KvStateMachine, RetriedCommandIsAppliedOnce) {
    StateMachine sm;
    sm.apply(cmd(7, 1, Op::Append, "k", "x"));
    sm.apply(cmd(7, 1, Op::Append, "k", "x"));   // the same request, in the log twice
    EXPECT_EQ(sm.get("k"), "x");
    EXPECT_EQ(sm.duplicates_suppressed(), 1u);
}

TEST(KvStateMachine, DuplicateGetsTheOriginalReply) {
    StateMachine sm;
    sm.apply(cmd(7, 1, Op::Put, "k", "old"));
    const Result first = sm.apply(cmd(7, 2, Op::Get, "k"));
    sm.apply(cmd(8, 1, Op::Put, "k", "new"));             // another client changes it
    EXPECT_EQ(sm.apply(cmd(7, 2, Op::Get, "k")), first);  // the retry sees what the original saw
}

TEST(KvStateMachine, OlderSeqIsIgnored) {
    StateMachine sm;
    sm.apply(cmd(7, 1, Op::Append, "k", "a"));
    sm.apply(cmd(7, 2, Op::Append, "k", "b"));
    sm.apply(cmd(7, 1, Op::Append, "k", "a"));   // a very late duplicate
    EXPECT_EQ(sm.get("k"), "ab");
}

TEST(KvStateMachine, ClientsAreIndependent) {
    StateMachine sm;
    sm.apply(cmd(1, 1, Op::Append, "k", "a"));
    sm.apply(cmd(2, 1, Op::Append, "k", "b"));   // same seq, different client: not a duplicate
    EXPECT_EQ(sm.get("k"), "ab");
}

}  // namespace
}  // namespace raftkv
