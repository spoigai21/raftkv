#pragma once

#include <chrono>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <optional>
#include <string>

#include <asio.hpp>

#include "raft/env.hpp"
#include "sim/rng.hpp"
#include "sim/sim_storage.hpp"

namespace raftkv::net {

struct NetConfig {
    raft::NodeId id = 0;
    std::map<raft::NodeId, asio::ip::tcp::endpoint> servers;   // every server, this one included
    bool listen = false;   // servers listen on servers[id]; clients only dial out
};

class Connection;

// The Env of a real process (implementation guide §3.1): everything, including the node,
// runs on the one thread that runs the io_context, so the node never sees concurrency.
//
//   now()      monotonic time since start
//   after()    asio::steady_timer
//   send()     TCP, length-prefixed frames (net/frame.hpp). Servers dial each other and
//              reconnect as needed; a reply to a client goes back on the connection its
//              request arrived on. If a destination is unreachable the message is dropped,
//              which Raft tolerates.
//   storage()  the Storage given at construction (FileStorage for a server)
//
// Exceptions thrown by the node (e.g. store::StorageFailure) propagate out of
// io_context::run(): the process should exit.
class AsioEnv final : public raft::Env {
public:
    // `storage` may be null for a client, which never uses it.
    AsioEnv(asio::io_context& io, NetConfig config, raft::Storage* storage);
    ~AsioEnv() override;
    AsioEnv(const AsioEnv&) = delete;
    AsioEnv& operator=(const AsioEnv&) = delete;

    // Starts listening (servers) and runs node.on_start() on the io thread.
    void start(raft::Node& node);
    // Closes every connection and timer; the io_context then runs out of work.
    void stop();

    // For tests: the port actually listened on (useful when configured with port 0).
    std::uint16_t listening_port() const;

    raft::Time now() const override;
    raft::TimerId after(raft::Duration delay, raft::TimerTag tag) override;
    void cancel(raft::TimerId id) override;
    void send(raft::Message m) override;
    raft::Storage& storage() override;
    std::uint64_t random() override { return rng_.next(); }
    void trace(std::string_view what) override;

private:
    struct Peer {
        std::shared_ptr<Connection> conn;
        bool connecting = false;
        std::chrono::steady_clock::time_point last_failure{};
        std::deque<std::string> waiting;   // frames queued while connecting
    };

    void accept_next();
    void connect(raft::NodeId peer);
    void adopt(std::shared_ptr<Connection> conn, std::optional<raft::NodeId> peer);
    void on_frame(const std::shared_ptr<Connection>& conn, raft::Message m);
    void on_closed(const std::shared_ptr<Connection>& conn);

    asio::io_context& io_;
    NetConfig config_;
    raft::Storage* storage_;
    sim::SimStorage unused_storage_;   // what storage() returns for a client
    raft::Node* node_ = nullptr;
    const std::chrono::steady_clock::time_point start_;
    sim::Rng rng_;
    std::optional<asio::ip::tcp::acceptor> acceptor_;

    std::map<raft::NodeId, Peer> peers_;   // outgoing connections to other servers
    std::map<raft::NodeId, std::weak_ptr<Connection>> inbound_;   // latest connection per sender
    std::map<Connection*, std::shared_ptr<Connection>> connections_;   // every open connection
    // Live timers; a null pointer is a zero-delay one, posted rather than timed.
    std::map<raft::TimerId, std::unique_ptr<asio::steady_timer>> timers_;
    raft::TimerId next_timer_ = 0;
    bool stopped_ = false;
};

}  // namespace raftkv::net
