#pragma once

// Toy nodes for exercising the simulator before there is any Raft.

#include <string>
#include <vector>

#include "raft/env.hpp"

namespace raftkv::test {

// Touches every part of Env: random timers, sends to random peers, storage writes with
// occasional syncs, and reloading storage on restart.
class GossipNode final : public raft::Node {
public:
    GossipNode(raft::NodeId me, std::vector<raft::NodeId> peers, raft::Env& env)
        : me_(me), peers_(std::move(peers)), env_(env) {}

    void on_start() override {
        log_size_ = env_.storage().load().log.size();
        arm();
    }

    void on_message(const raft::Message& m) override {
        raft::LogEntry e{.term = 0, .index = ++log_size_, .command = m.payload};
        env_.storage().append({&e, 1});
        ++received_;
    }

    void on_timer(raft::TimerId, raft::TimerTag) override {
        const raft::NodeId to = peers_[env_.random() % peers_.size()];
        env_.send({.to = to, .method = "Gossip",
                   .payload = std::to_string(me_) + ":" + std::to_string(sent_++)});
        if (sent_ % 3 == 0) env_.storage().sync();
        arm();
    }

    std::uint64_t received() const { return received_; }

private:
    void arm() { env_.after(raft::Duration(5'000 + env_.random() % 45'000), 0); }

    raft::NodeId me_;
    std::vector<raft::NodeId> peers_;
    raft::Env& env_;
    std::uint64_t log_size_ = 0;
    std::uint64_t sent_ = 0;
    std::uint64_t received_ = 0;
};

// Records what arrives and fires timers on request; tests drive it directly.
class ProbeNode final : public raft::Node {
public:
    explicit ProbeNode(raft::Env& env) : env_(env) {}

    void on_start() override { ++starts; }
    void on_message(const raft::Message& m) override { messages.push_back(m.payload); }
    void on_timer(raft::TimerId, raft::TimerTag tag) override { timers.push_back(tag); }

    raft::Env& env() { return env_; }

    int starts = 0;
    std::vector<std::string> messages;
    std::vector<raft::TimerTag> timers;

private:
    raft::Env& env_;
};

}  // namespace raftkv::test
