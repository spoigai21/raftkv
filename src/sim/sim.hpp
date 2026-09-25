#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "raft/env.hpp"
#include "sim/rng.hpp"
#include "sim/sim_storage.hpp"

namespace raftkv::sim {

// Network faults, read at the moment each message is sent (and partitions again at delivery).
// Change them at any time, typically from a scripted event.
struct Faults {
    double drop_rate = 0.0;
    raft::Duration delay_min{1000};   // 1 ms
    raft::Duration delay_max{1000};
    bool reorder = false;   // false: each (from, to) link delivers in send order
    // Empty: everyone can talk. Otherwise two nodes can talk only if they are in the same
    // group, and a node listed in no group is isolated. {{1, 2}, {3}} cuts node 3 off.
    std::vector<std::vector<raft::NodeId>> partitions;
};

struct SimStats {
    std::uint64_t sent = 0;
    std::uint64_t delivered = 0;
    std::uint64_t dropped_loss = 0;
    std::uint64_t dropped_partition = 0;
    std::uint64_t dropped_down = 0;
};

// A deterministic discrete-event simulator (implementation guide §1.2). One priority queue of
// (time, seq, event); time jumps to the next event and nothing ever sleeps. Given the same
// seed, the same nodes and the same script, every run produces a byte-identical event log.
class Sim {
public:
    using NodeFactory =
        std::function<std::unique_ptr<raft::Node>(raft::NodeId, raft::Env&)>;

    Sim(std::uint64_t seed, std::vector<raft::NodeId> ids, NodeFactory factory);
    Sim(const Sim&) = delete;
    Sim& operator=(const Sim&) = delete;

    raft::Time now() const { return now_; }
    std::uint64_t seed() const { return seed_; }
    Rng& rng() { return rng_; }
    Faults& faults() { return faults_; }
    const SimStats& stats() const { return stats_; }

    // Boots every node (in id order) at the current time.
    void start();

    // Runs `fn` at virtual time `at` (or now, if `at` is in the past), logged as `label`.
    void schedule(raft::Time at, std::string label, std::function<void()> fn);

    // Runs every event due at or before `t`, then sets now() to `t`.
    void run_until(raft::Time t);
    void run_for(raft::Duration d) { run_until(now_ + d); }
    // Runs until `done()` holds or `deadline` passes. Returns whether `done()` holds.
    bool run_until(const std::function<bool()>& done, raft::Time deadline);

    // Node-level faults. crash drops the node's memory, timers and unsynced writes;
    // restart rebuilds it from what storage kept. pause stops it processing anything, and
    // what arrives meanwhile is handled in arrival order on resume.
    void crash(raft::NodeId id,
               SimStorage::CrashMode mode = SimStorage::CrashMode::DropUnsynced);
    void restart(raft::NodeId id);
    void pause(raft::NodeId id);
    void resume(raft::NodeId id);
    bool is_up(raft::NodeId id) const { return slot(id).node != nullptr; }
    bool is_paused(raft::NodeId id) const { return slot(id).paused; }

    raft::Node* node(raft::NodeId id) { return slot(id).node.get(); }
    template <class T>
    T& node_as(raft::NodeId id) {
        auto* n = dynamic_cast<T*>(node(id));
        if (n == nullptr) throw std::logic_error("sim: node is down or has another type");
        return *n;
    }
    SimStorage& storage(raft::NodeId id) { return slot(id).storage; }

    // The event log: one line per event. log_hash() covers every line even when lines are
    // not kept, so long runs can still be compared cheaply.
    const std::vector<std::string>& event_log() const { return log_; }
    std::uint64_t log_hash() const { return log_hash_; }
    void keep_log_lines(bool keep) { keep_log_lines_ = keep; }
    std::string log_tail(std::size_t n) const;

    // How messages appear in the log after "send 1->2 ". Default: method and payload size.
    void describe_messages_with(std::function<std::string(const raft::Message&)> fn) {
        describe_ = std::move(fn);
    }

    // Runs after every event. Tests use it to check invariants continuously, so a violation
    // is caught at the event that caused it rather than at the end of the run.
    void after_each_event(std::function<void()> fn) { after_each_event_ = std::move(fn); }

private:
    class NodeEnv final : public raft::Env {
    public:
        NodeEnv(Sim& sim, raft::NodeId id) : sim_(sim), id_(id) {}
        raft::Time now() const override { return sim_.now_; }
        raft::TimerId after(raft::Duration delay, raft::TimerTag tag) override;
        void cancel(raft::TimerId id) override;
        void send(raft::Message m) override;
        raft::Storage& storage() override;
        std::uint64_t random() override { return sim_.rng_.next(); }
        void trace(std::string_view what) override;

    private:
        Sim& sim_;
        raft::NodeId id_;
    };

    struct NodeSlot {
        std::unique_ptr<NodeEnv> env;
        std::unique_ptr<raft::Node> node;   // null while crashed or before start()
        SimStorage storage;
        std::uint64_t incarnation = 0;
        bool paused = false;
        std::map<raft::TimerId, raft::TimerTag> timers;   // live timers
        std::vector<std::function<void()>> deferred;      // what arrived while paused
    };

    struct Event {
        raft::Time at;
        std::uint64_t seq;
        std::function<void()> fn;
    };

    NodeSlot& slot(raft::NodeId id);
    const NodeSlot& slot(raft::NodeId id) const;

    void push(raft::Time at, std::function<void()> fn);
    bool step();   // runs the earliest event; false when the queue is empty
    void log(std::string_view what);

    bool connected(raft::NodeId a, raft::NodeId b) const;
    void send(raft::Message m);
    void deliver(raft::Message m);   // network hop done: partition check, then receive()
    void receive(raft::Message m);   // at the node: dropped if down, deferred if paused
    void fire_timer(raft::NodeId id, raft::TimerId timer, std::uint64_t incarnation);
    void boot(raft::NodeId id, std::string_view why);
    std::string describe(const raft::Message& m) const;

    std::uint64_t seed_;
    Rng rng_;
    NodeFactory factory_;
    Faults faults_;
    SimStats stats_;

    raft::Time now_{};
    std::uint64_t next_seq_ = 0;
    raft::TimerId next_timer_id_ = 0;
    std::vector<Event> queue_;   // min-heap on (at, seq)
    std::map<raft::NodeId, NodeSlot> nodes_;
    std::map<std::pair<raft::NodeId, raft::NodeId>, raft::Time> link_last_;   // FIFO links

    std::vector<std::string> log_;
    std::uint64_t log_hash_ = 0xcbf29ce484222325ULL;   // FNV-1a offset basis
    bool keep_log_lines_ = true;
    std::function<std::string(const raft::Message&)> describe_;
    std::function<void()> after_each_event_;
};

}  // namespace raftkv::sim
