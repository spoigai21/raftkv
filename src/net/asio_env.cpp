#include "net/asio_env.hpp"

#include <array>
#include <cstdio>
#include <format>
#include <functional>
#include <random>

#include "net/frame.hpp"

namespace raftkv::net {

using asio::ip::tcp;

namespace {
constexpr std::size_t kMaxQueuedFrames = 1024;   // per connection; beyond this we drop
constexpr auto kReconnectBackoff = std::chrono::milliseconds(100);
}  // namespace

// One TCP connection carrying frames both ways. Owned by AsioEnv::connections_; async
// operations hold a shared_ptr so the object outlives any handler still in flight.
class Connection : public std::enable_shared_from_this<Connection> {
public:
    using OnFrame = std::function<void(const std::shared_ptr<Connection>&, raft::Message)>;
    using OnClosed = std::function<void(const std::shared_ptr<Connection>&)>;

    Connection(tcp::socket socket, OnFrame on_frame, OnClosed on_closed)
        : socket_(std::move(socket)), on_frame_(std::move(on_frame)), on_closed_(std::move(on_closed)) {
        asio::error_code ignored;
        socket_.set_option(tcp::no_delay(true), ignored);
    }

    void start() { read_more(); }

    void write(std::string frame) {
        if (closed_) return;
        if (queue_.size() >= kMaxQueuedFrames) return;   // a stuck peer: drop, like the network would
        queue_.push_back(std::move(frame));
        if (queue_.size() == 1) write_next();
    }

    void close() {
        if (closed_) return;
        closed_ = true;
        asio::error_code ignored;
        socket_.shutdown(tcp::socket::shutdown_both, ignored);
        socket_.close(ignored);
        // queue_ is left alone: an async_write may still point into its front frame. The
        // frames are freed with the Connection, once the last handler holding it has run.
        if (on_closed_) on_closed_(shared_from_this());
    }

private:
    void read_more() {
        socket_.async_read_some(asio::buffer(buf_), [self = shared_from_this()](asio::error_code ec, std::size_t n) {
            if (ec) {
                self->close();
                return;
            }
            self->reader_.feed(std::as_bytes(std::span(self->buf_).first(n)));
            while (true) {
                auto m = self->reader_.next();
                if (!m) {   // framing is lost; nothing on this connection can be trusted now
                    self->close();
                    return;
                }
                if (!*m) break;
                self->on_frame_(self, std::move(**m));
                if (self->closed_) return;
            }
            self->read_more();
        });
    }

    void write_next() {
        asio::async_write(socket_, asio::buffer(queue_.front()),
                          [self = shared_from_this()](asio::error_code ec, std::size_t) {
                              // The read side may have closed the connection between this
                              // write finishing and this handler running (postmortem 002).
                              if (self->closed_) return;
                              if (ec) {
                                  self->close();
                                  return;
                              }
                              self->queue_.pop_front();
                              if (!self->queue_.empty()) self->write_next();
                          });
    }

    tcp::socket socket_;
    OnFrame on_frame_;
    OnClosed on_closed_;
    FrameReader reader_;
    std::array<char, 64 * 1024> buf_{};
    std::deque<std::string> queue_;
    bool closed_ = false;
};

AsioEnv::AsioEnv(asio::io_context& io, NetConfig config, raft::Storage* storage)
    : io_(io), config_(std::move(config)), storage_(storage), start_(std::chrono::steady_clock::now()),
      rng_(std::random_device{}() ^ (static_cast<std::uint64_t>(std::random_device{}()) << 32)) {}

AsioEnv::~AsioEnv() { stop(); }

void AsioEnv::start(raft::Node& node) {
    node_ = &node;
    if (config_.listen) {
        const auto it = config_.servers.find(config_.id);
        if (it == config_.servers.end()) throw std::invalid_argument("this node is not in the cluster config");
        acceptor_.emplace(io_);
        acceptor_->open(it->second.protocol());
        acceptor_->set_option(tcp::acceptor::reuse_address(true));
        acceptor_->bind(it->second);
        acceptor_->listen();
        accept_next();
    }
    asio::post(io_, [this] {
        if (!stopped_) node_->on_start();
    });
}

void AsioEnv::stop() {
    if (stopped_) return;
    stopped_ = true;
    if (acceptor_) {
        asio::error_code ignored;
        acceptor_->close(ignored);
    }
    for (auto& [id, t] : timers_) t->cancel();
    timers_.clear();
    auto open = std::move(connections_);
    for (auto& [ptr, conn] : open) conn->close();
    peers_.clear();
    inbound_.clear();
}

std::uint16_t AsioEnv::listening_port() const { return acceptor_ ? acceptor_->local_endpoint().port() : 0; }

raft::Time AsioEnv::now() const {
    return raft::Time{std::chrono::duration_cast<raft::Duration>(std::chrono::steady_clock::now() - start_)};
}

raft::TimerId AsioEnv::after(raft::Duration delay, raft::TimerTag tag) {
    const raft::TimerId id = ++next_timer_;
    auto timer = std::make_unique<asio::steady_timer>(io_, delay);
    timer->async_wait([this, id, tag](asio::error_code ec) {
        if (ec || stopped_) return;
        auto it = timers_.find(id);
        if (it == timers_.end()) return;   // cancelled after it fired but before it ran
        timers_.erase(it);
        node_->on_timer(id, tag);
    });
    timers_.emplace(id, std::move(timer));
    return id;
}

void AsioEnv::cancel(raft::TimerId id) {
    auto it = timers_.find(id);
    if (it == timers_.end()) return;
    it->second->cancel();
    timers_.erase(it);
}

void AsioEnv::send(raft::Message m) {
    if (stopped_) return;
    m.from = config_.id;
    std::string frame = encode_frame(m);

    if (m.to != config_.id && config_.servers.contains(m.to)) {
        Peer& p = peers_[m.to];
        if (p.conn) {
            p.conn->write(std::move(frame));
            return;
        }
        if (p.waiting.size() < kMaxQueuedFrames) p.waiting.push_back(std::move(frame));
        connect(m.to);
        return;
    }
    // Not a server we dial: reply on the connection this node last heard from it on.
    if (auto it = inbound_.find(m.to); it != inbound_.end()) {
        if (auto conn = it->second.lock()) conn->write(std::move(frame));
    }
}

raft::Storage& AsioEnv::storage() { return storage_ ? *storage_ : unused_storage_; }

void AsioEnv::trace(std::string_view what) {
    const auto us = now().since_start.count();
    std::fprintf(stderr, "[%10.6f] node %u: %.*s\n", static_cast<double>(us) / 1e6, config_.id,
                 static_cast<int>(what.size()), what.data());
}

void AsioEnv::accept_next() {
    acceptor_->async_accept([this](asio::error_code ec, tcp::socket socket) {
        if (stopped_) return;
        if (!ec) {
            auto conn = std::make_shared<Connection>(
                std::move(socket), [this](const auto& c, raft::Message m) { on_frame(c, std::move(m)); },
                [this](const auto& c) { on_closed(c); });
            adopt(conn, std::nullopt);
        }
        accept_next();
    });
}

void AsioEnv::connect(raft::NodeId peer) {
    Peer& p = peers_[peer];
    if (p.connecting || p.conn) return;
    if (std::chrono::steady_clock::now() - p.last_failure < kReconnectBackoff) {
        p.waiting.clear();   // it was down a moment ago: drop rather than hammer it
        return;
    }
    p.connecting = true;
    auto socket = std::make_shared<tcp::socket>(io_);
    socket->async_connect(config_.servers.at(peer), [this, peer, socket](asio::error_code ec) {
        if (stopped_) return;
        Peer& p = peers_[peer];
        p.connecting = false;
        if (ec) {
            p.last_failure = std::chrono::steady_clock::now();
            p.waiting.clear();
            return;
        }
        auto conn = std::make_shared<Connection>(
            std::move(*socket), [this](const auto& c, raft::Message m) { on_frame(c, std::move(m)); },
            [this](const auto& c) { on_closed(c); });
        adopt(conn, peer);
        for (std::string& frame : p.waiting) conn->write(std::move(frame));
        p.waiting.clear();
    });
}

void AsioEnv::adopt(std::shared_ptr<Connection> conn, std::optional<raft::NodeId> peer) {
    connections_.emplace(conn.get(), conn);
    if (peer) peers_[*peer].conn = conn;
    conn->start();
}

void AsioEnv::on_frame(const std::shared_ptr<Connection>& conn, raft::Message m) {
    if (stopped_) return;
    if (m.to != config_.id) return;   // misrouted
    inbound_[m.from] = conn;
    node_->on_message(m);
}

void AsioEnv::on_closed(const std::shared_ptr<Connection>& conn) {
    connections_.erase(conn.get());
    for (auto& [id, p] : peers_) {
        if (p.conn == conn) p.conn.reset();
    }
    std::erase_if(inbound_, [&](const auto& kv) { return kv.second.lock() == conn; });
}

}  // namespace raftkv::net
