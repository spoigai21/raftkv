#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <vector>

#include "kv/messages.hpp"
#include "raft/env.hpp"

namespace raftkv::kv {

// The request timeout adapts to measured round trips, as TCP's retransmission timeout does
// (RFC 6298): smoothed RTT + 4 × its variation, between min_timeout and max_timeout. Until
// the first sample it is initial_timeout. Setting min and max equal to initial gives a fixed
// timeout.
struct ClientConfig {
    std::vector<raft::NodeId> servers;
    raft::Duration initial_timeout{500'000};   // implementation guide §3.5
    // Well above a single fsync (4 ms here), so latency spikes under load do not cause
    // spurious resends, which would become duplicate proposals and more fsync load.
    raft::Duration min_timeout{100'000};
    raft::Duration max_timeout{1'000'000};
    raft::Duration retry_backoff{20'000};      // after NotLeader with no hint, e.g. mid-election
};

struct ClientStats {
    std::uint64_t completed = 0;
    std::uint64_t timeouts = 0;
    std::uint64_t not_leader = 0;
};

// A key-value client with one request outstanding at a time. It finds the leader by
// following NotLeader hints. When a request times out it resends it to the same server once
// (the likeliest cause is a lost message), then moves on to the next server. Every retry
// carries the same (client_id, seq), which is what makes retries safe.
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
    // The server the next (or current) request goes to: after a success, the leader.
    raft::NodeId current_server() const { return config_.servers[target_]; }
    raft::Duration current_timeout() const { return timeout_; }
    // What a new request starts with: srtt + 4 * rttvar, clamped.
    raft::Duration base_timeout() const { return base_timeout_; }
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
    void record_rtt(raft::Duration sample);

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

    // Adaptive timeout state (RFC 6298). Backoff applies within one request: each new request
    // starts again from base_timeout_.
    raft::Duration base_timeout_;
    raft::Duration timeout_;
    std::optional<double> srtt_us_;
    double rttvar_us_ = 0;
    raft::Time sent_at_{};
    bool retried_ = false;          // the outstanding request has been resent (Karn's rule)
    int timeouts_on_target_ = 0;
};

}  // namespace raftkv::kv
