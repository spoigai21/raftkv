#include "sim/sim.hpp"

#include <algorithm>
#include <format>

namespace raftkv::sim {

namespace {

// Min-heap order: the earliest time first, then the earliest scheduled.
bool later(const auto& a, const auto& b) {
    return std::pair(a.at, a.seq) > std::pair(b.at, b.seq);
}

std::string describe(const raft::Message& m) {
    return std::format("{}->{} {} len={}", m.from, m.to, m.method, m.payload.size());
}

}  // namespace

Sim::Sim(std::uint64_t seed, std::vector<raft::NodeId> ids, NodeFactory factory)
    : seed_(seed), rng_(seed), factory_(std::move(factory)) {
    for (raft::NodeId id : ids) {
        auto [it, inserted] = nodes_.try_emplace(id);
        if (!inserted) throw std::logic_error(std::format("sim: duplicate node id {}", id));
        it->second.env = std::make_unique<NodeEnv>(*this, id);
    }
}

void Sim::start() {
    for (auto& [id, s] : nodes_) {
        if (s.node == nullptr) boot(id, "start");
    }
}

void Sim::schedule(raft::Time at, std::string label, std::function<void()> fn) {
    push(std::max(at, now_), [this, label = std::move(label), fn = std::move(fn)] {
        log(std::format("script {}", label));
        fn();
    });
}

void Sim::run_until(raft::Time t) {
    while (!queue_.empty() && queue_.front().at <= t) step();
    now_ = std::max(now_, t);
}

bool Sim::run_until(const std::function<bool()>& done, raft::Time deadline) {
    while (!done()) {
        if (queue_.empty() || queue_.front().at > deadline) {
            now_ = std::max(now_, deadline);
            return done();
        }
        step();
    }
    return true;
}

void Sim::crash(raft::NodeId id, SimStorage::CrashMode mode) {
    NodeSlot& s = slot(id);
    if (s.node == nullptr) throw std::logic_error(std::format("sim: crash of node {}, already down", id));
    s.node.reset();
    ++s.incarnation;
    s.timers.clear();
    s.deferred.clear();
    s.paused = false;
    const std::size_t lost = s.storage.crash(mode, rng_);
    log(std::format("crash {} lost_unsynced_ops={}", id, lost));
}

void Sim::restart(raft::NodeId id) {
    if (slot(id).node != nullptr) throw std::logic_error(std::format("sim: restart of node {}, already up", id));
    boot(id, "restart");
}

void Sim::pause(raft::NodeId id) {
    NodeSlot& s = slot(id);
    if (s.node == nullptr || s.paused) return;
    s.paused = true;
    log(std::format("pause {}", id));
}

void Sim::resume(raft::NodeId id) {
    NodeSlot& s = slot(id);
    if (!s.paused) return;
    s.paused = false;
    log(std::format("resume {} deferred={}", id, s.deferred.size()));
    // Everything that piled up runs now, in the order it arrived, as ordinary events.
    for (auto& fn : std::exchange(s.deferred, {})) push(now_, std::move(fn));
}

std::string Sim::log_tail(std::size_t n) const {
    std::string out;
    for (std::size_t i = log_.size() - std::min(n, log_.size()); i < log_.size(); ++i) {
        out += log_[i];
        out += '\n';
    }
    return out;
}

Sim::NodeSlot& Sim::slot(raft::NodeId id) {
    auto it = nodes_.find(id);
    if (it == nodes_.end()) throw std::logic_error(std::format("sim: unknown node {}", id));
    return it->second;
}

const Sim::NodeSlot& Sim::slot(raft::NodeId id) const {
    auto it = nodes_.find(id);
    if (it == nodes_.end()) throw std::logic_error(std::format("sim: unknown node {}", id));
    return it->second;
}

void Sim::push(raft::Time at, std::function<void()> fn) {
    queue_.push_back(Event{at, next_seq_++, std::move(fn)});
    std::push_heap(queue_.begin(), queue_.end(), later<Event, Event>);
}

bool Sim::step() {
    if (queue_.empty()) return false;
    std::pop_heap(queue_.begin(), queue_.end(), later<Event, Event>);
    Event ev = std::move(queue_.back());
    queue_.pop_back();
    now_ = ev.at;
    ev.fn();   // may push more events; `ev` is already out of the queue
    return true;
}

void Sim::log(std::string_view what) {
    std::string line = std::format("t={:>10}us {}", now_.since_start.count(), what);
    for (unsigned char c : line) log_hash_ = (log_hash_ ^ c) * 0x100000001b3ULL;
    log_hash_ = (log_hash_ ^ static_cast<unsigned char>('\n')) * 0x100000001b3ULL;
    if (keep_log_lines_) log_.push_back(std::move(line));
}

bool Sim::connected(raft::NodeId a, raft::NodeId b) const {
    if (a == b || faults_.partitions.empty()) return true;
    for (const auto& group : faults_.partitions) {
        const bool has_a = std::ranges::find(group, a) != group.end();
        const bool has_b = std::ranges::find(group, b) != group.end();
        if (has_a || has_b) return has_a && has_b;
    }
    return false;   // neither is listed: both isolated
}

void Sim::send(raft::Message m) {
    ++stats_.sent;
    if (!connected(m.from, m.to)) {
        ++stats_.dropped_partition;
        log(std::format("drop(partition) {}", describe(m)));
        return;
    }
    if (rng_.chance(faults_.drop_rate)) {
        ++stats_.dropped_loss;
        log(std::format("drop(loss) {}", describe(m)));
        return;
    }
    const auto lo = static_cast<std::uint64_t>(faults_.delay_min.count());
    const auto hi = static_cast<std::uint64_t>(std::max(faults_.delay_min, faults_.delay_max).count());
    raft::Time at = now_ + raft::Duration(static_cast<raft::Duration::rep>(rng_.between(lo, hi)));
    if (!faults_.reorder) {
        // Equal times are fine: ties run in scheduling order, which is send order.
        raft::Time& last = link_last_[{m.from, m.to}];
        at = std::max(at, last);
        last = at;
    }
    log(std::format("send {} arrive=t{}us", describe(m), at.since_start.count()));
    push(at, [this, m = std::move(m)]() mutable { deliver(std::move(m)); });
}

void Sim::deliver(raft::Message m) {
    if (!connected(m.from, m.to)) {
        ++stats_.dropped_partition;
        log(std::format("drop(partition) {}", describe(m)));
        return;
    }
    receive(std::move(m));
}

void Sim::receive(raft::Message m) {
    NodeSlot& s = slot(m.to);
    if (s.node == nullptr) {
        ++stats_.dropped_down;
        log(std::format("drop(down) {}", describe(m)));
        return;
    }
    if (s.paused) {
        // The packet reached the paused process; it reads it on resume. Partitions are not
        // checked again: the packet is already in the node's socket buffer.
        s.deferred.push_back([this, m = std::move(m)]() mutable { receive(std::move(m)); });
        return;
    }
    ++stats_.delivered;
    log(std::format("deliver {}", describe(m)));
    s.node->on_message(m);
}

void Sim::fire_timer(raft::NodeId id, raft::TimerId timer, std::uint64_t incarnation) {
    NodeSlot& s = slot(id);
    if (s.incarnation != incarnation) return;   // set by a node that has since crashed
    auto it = s.timers.find(timer);
    if (it == s.timers.end()) return;           // cancelled
    if (s.paused) {
        s.deferred.push_back([this, id, timer, incarnation] { fire_timer(id, timer, incarnation); });
        return;
    }
    const raft::TimerTag tag = it->second;
    s.timers.erase(it);
    log(std::format("timer {} id={} tag={}", id, timer, tag));
    s.node->on_timer(timer, tag);
}

void Sim::boot(raft::NodeId id, std::string_view why) {
    NodeSlot& s = slot(id);
    s.node = factory_(id, *s.env);
    log(std::format("{} {}", why, id));
    s.node->on_start();
}

raft::TimerId Sim::NodeEnv::after(raft::Duration delay, raft::TimerTag tag) {
    NodeSlot& s = sim_.slot(id_);
    const raft::TimerId timer = ++sim_.next_timer_id_;
    s.timers.emplace(timer, tag);
    sim_.push(sim_.now_ + std::max(delay, raft::Duration{0}),
              [&sim = sim_, id = id_, timer, inc = s.incarnation] { sim.fire_timer(id, timer, inc); });
    return timer;
}

void Sim::NodeEnv::cancel(raft::TimerId timer) { sim_.slot(id_).timers.erase(timer); }

void Sim::NodeEnv::send(raft::Message m) {
    m.from = id_;   // a node cannot forge its sender
    sim_.send(std::move(m));
}

raft::Storage& Sim::NodeEnv::storage() { return sim_.slot(id_).storage; }

}  // namespace raftkv::sim
