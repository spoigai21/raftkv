#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <vector>

#include "kv/messages.hpp"
#include "raft/env.hpp"

namespace raftkv::kv {

struct ClientConfig {
    std::vector<raft::NodeId> servers;
    raft::Duration request_timeout{500'000};   // implementation guide §3.5
    raft::Duration retry_backoff{20'000};      // after NotLeader with no hint, e.g. mid-election
};

struct ClientStats {
    std::uint64_t completed = 0;
    std::uint64_t timeouts = 0;
    std::uint64_t not_leader = 0;
};

// A key-value client with one request outstanding at a time. It finds the leader by
// following NotLeader hints, and moves on to another server when a request times out,
// always retrying with the same (client_id, seq), which is what makes retries safe.
//
// It is a Node, so it runs in the simulator (and on a real Env) like a server does.
class Client final : public raft::Node {
public:
    using Callback = std::function<void(const Result&)>;

    Client(ClientConfig config, raft::Env& env);

    // Starts an operation; `done` runs once it has taken effect. Only one at a time.
    void get(std::string key, Callback done);
    void put(std::string key, std::string value, Callback done);
    void append(std::string key, std::string value, Callback done);

    bool busy() const { return outstanding_.has_value(); }
    std::uint64_t client_id() const { return client_id_; }
    const ClientStats& stats() const { return stats_; }

    void on_start() override {}
    void on_message(const raft::Message& m) override;
    void on_timer(raft::TimerId id, raft::TimerTag tag) override;

private:
    enum : raft::TimerTag { kTimeout = 1, kBackoff = 2 };

    void start(Op op, std::string key, std::string value, Callback done);
    void send_now();
    void try_next_server();
    void cancel_timers();

    ClientConfig config_;
    raft::Env& env_;
    std::uint64_t client_id_;
    std::uint64_t next_seq_ = 1;
    std::size_t target_ = 0;   // index into config_.servers
    std::optional<Command> outstanding_;
    Callback done_;
    std::optional<raft::TimerId> timeout_timer_;
    std::optional<raft::TimerId> backoff_timer_;
    ClientStats stats_;
};

}  // namespace raftkv::kv
