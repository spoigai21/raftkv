// FileStorage against power loss: random workloads on a model disk (power_loss_io.hpp) that
// loses the power at a random file operation, then recovery checked against what the Storage
// contract promised to keep.

#include <gtest/gtest.h>

#include <algorithm>
#include <format>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "power_loss_io.hpp"
#include "sim/rng.hpp"
#include "store/file_storage.hpp"
#include "store/log_codec.hpp"
#include "test_seeds.hpp"

namespace raftkv {
namespace {

using raft::Index;
using raft::LogEntry;
using raft::NodeId;
using raft::Snapshot;
using raft::Term;
using store::FileStorage;
using store::LogRecord;
using store::TruncateFrom;
using test::PowerCut;
using test::PowerLossIo;

// ---- planted bugs: a correct FileStorage minus one fsync -----------------------------------

enum class Bug { None, NoDirSync, NoTmpSync, NoLogSync };

// Passes every operation to the model disk, except the syncs a bug leaves out.
class BuggyIo final : public store::Io {
public:
    BuggyIo(std::shared_ptr<PowerLossIo> disk, Bug bug) : disk_(std::move(disk)), bug_(bug) {}

    bool exists(const std::filesystem::path& p) override { return disk_->exists(p); }
    tl::expected<std::vector<std::byte>, std::string> read(const std::filesystem::path& p) override {
        return disk_->read(p);
    }
    void create_dirs(const std::filesystem::path& d) override { disk_->create_dirs(d); }
    void remove(const std::filesystem::path& p) override { disk_->remove(p); }
    void write_new(const std::filesystem::path& p, std::span<const std::byte> b) override { disk_->write_new(p, b); }
    void append(const std::filesystem::path& p, std::span<const std::byte> b) override { disk_->append(p, b); }
    void truncate(const std::filesystem::path& p, std::uint64_t size) override { disk_->truncate(p, size); }
    void rename(const std::filesystem::path& from, const std::filesystem::path& to) override {
        disk_->rename(from, to);
    }
    void sync(const std::filesystem::path& p) override {
        if (bug_ == Bug::NoTmpSync && p.extension() == ".tmp") return;
        if (bug_ == Bug::NoLogSync && p.filename() == "log") return;
        disk_->sync(p);
    }
    void sync_dir(const std::filesystem::path& d) override {
        if (bug_ != Bug::NoDirSync) disk_->sync_dir(d);
    }

private:
    std::shared_ptr<PowerLossIo> disk_;
    Bug bug_;
};

// ---- what recovery may return -------------------------------------------------------------

std::vector<LogEntry> apply_records(std::vector<LogEntry> log, std::span<const LogRecord> records) {
    for (const LogRecord& r : records) {
        if (const auto* e = std::get_if<LogEntry>(&r)) {
            log.push_back(*e);
        } else {
            const Index from = std::get<TruncateFrom>(r).index;
            std::erase_if(log, [from](const LogEntry& e) { return e.index >= from; });
        }
    }
    return log;
}

std::vector<LogEntry> after(std::vector<LogEntry> log, const std::optional<Snapshot>& snap) {
    const Index covered = snap ? snap->last_included_index : 0;
    std::erase_if(log, [covered](const LogEntry& e) { return e.index <= covered; });
    return log;
}

// The Storage contract, tracked alongside the real FileStorage:
//   - save_hard_state() and save_snapshot() are durable when they return;
//   - append() and truncate_suffix() are durable once a later sync() returns, and until then
//     a power cut keeps some prefix of them, in order.
// An operation the power went out on may or may not have happened.
struct Oracle {
    Term term = 0;
    std::optional<NodeId> vote;
    std::optional<Snapshot> snap;
    std::vector<LogEntry> synced;      // the log as of the last sync, after `snap`
    std::vector<LogRecord> pending;    // written since, in order (including an interrupted one)

    std::optional<std::pair<Term, std::optional<NodeId>>> interrupted_hard_state;
    std::optional<Snapshot> interrupted_snapshot;
    std::vector<LogEntry> interrupted_snapshot_log;   // what that save_snapshot would leave

    std::vector<LogEntry> log() const { return after(apply_records(synced, pending), snap); }

    raft::PersistentState now() const {
        return {.current_term = term, .voted_for = vote, .snapshot = snap, .log = log()};
    }

    // Every state recovery may legitimately produce.
    std::vector<raft::PersistentState> allowed() const {
        std::vector<std::pair<Term, std::optional<NodeId>>> hard{{term, vote}};
        if (interrupted_hard_state) hard.push_back(*interrupted_hard_state);
        std::vector<std::pair<std::optional<Snapshot>, std::vector<LogEntry>>> logs;
        for (std::size_t k = 0; k <= pending.size(); ++k) {
            const auto prefix = apply_records(synced, std::span(pending).first(k));
            logs.emplace_back(snap, after(prefix, snap));
            // The new snapshot landed, the old log (the rewrite had not) under it.
            if (interrupted_snapshot) logs.emplace_back(interrupted_snapshot, after(prefix, interrupted_snapshot));
        }
        if (interrupted_snapshot) logs.emplace_back(interrupted_snapshot, interrupted_snapshot_log);

        std::vector<raft::PersistentState> out;
        for (const auto& [t, v] : hard) {
            for (const auto& [s, l] : logs) out.push_back({.current_term = t, .voted_for = v, .snapshot = s, .log = l});
        }
        return out;
    }

    // After recovery, what came back is the new durable state.
    void recovered(const raft::PersistentState& s) {
        *this = Oracle{};
        term = s.current_term;
        vote = s.voted_for;
        snap = s.snapshot;
        synced = s.log;
    }
};

std::string describe(const raft::PersistentState& s) {
    std::string out = std::format("term {} vote {} ", s.current_term, s.voted_for ? std::to_string(*s.voted_for) : "-");
    if (s.snapshot) {
        out += std::format("snapshot ({}, {}, {} bytes) ", s.snapshot->last_included_index,
                           s.snapshot->last_included_term, s.snapshot->data.size());
    }
    out += "log [";
    for (const LogEntry& e : s.log) out += std::format(" {}@{}", e.index, e.term);
    return out + " ]";
}

std::string random_bytes(sim::Rng& rng, std::size_t n) {
    std::string s(n, '\0');
    for (char& c : s) c = static_cast<char>(rng.between(0, 255));
    return s;
}

// A command or snapshot, sometimes big enough to span several disk pages.
std::string payload(sim::Rng& rng) {
    return random_bytes(rng, rng.chance(0.1) ? rng.between(2000, 9000) : rng.between(0, 40));
}

// Runs `rounds` rounds of: open (recovering), check the recovery against the oracle, then
// random operations until the power goes out. Returns the first violation found.
std::optional<std::string> run(std::uint64_t seed, Bug bug, int rounds = 12) {
    sim::Rng rng(seed);
    auto disk = std::make_shared<PowerLossIo>();
    auto io = bug == Bug::None ? std::shared_ptr<store::Io>(disk) : std::make_shared<BuggyIo>(disk, bug);
    const std::filesystem::path dir = "/data";
    Oracle oracle;

    for (int round = 0; round < rounds; ++round) {
        const std::string where = std::format("seed {} round {}", seed, round);
        // Recover, through power cuts during recovery itself.
        disk->cut_after(rng.between(0, 60));
        std::unique_ptr<FileStorage> storage;
        while (!storage) {
            try {
                auto opened = FileStorage::open(dir, FileStorage::Sync::Durable, io);
                if (!opened) return std::format("{}: recovery failed: {}", where, opened.error());
                storage = std::move(*opened);
            } catch (const PowerCut&) {
                disk->crash(rng);
            }
        }
        const raft::PersistentState got = storage->load();
        const auto allowed = oracle.allowed();
        if (std::ranges::find(allowed, got) == allowed.end()) {
            std::string msg = std::format("{}: recovered\n  {}\nbut the contract allows only", where, describe(got));
            for (const auto& a : allowed) msg += "\n  " + describe(a);
            return msg;
        }
        oracle.recovered(got);

        if (round + 1 == rounds) break;
        disk->cut_after(rng.between(0, 80));
        try {
            for (int op = 0; op < 40; ++op) {
                const auto view = oracle.log();
                const Index snap_index = oracle.snap ? oracle.snap->last_included_index : 0;
                const Index last = view.empty() ? snap_index : view.back().index;
                const Term last_term =
                    view.empty() ? (oracle.snap ? oracle.snap->last_included_term : 0) : view.back().term;
                const auto choice = rng.between(0, 99);
                if (choice < 15) {
                    const Term t = oracle.term + rng.between(0, 1);
                    const std::optional<NodeId> v =
                        rng.chance(0.3) ? std::nullopt : std::optional<NodeId>(rng.between(1, 5));
                    oracle.interrupted_hard_state = {t, v};
                    storage->save_hard_state(t, v);
                    oracle.term = t;
                    oracle.vote = v;
                    oracle.interrupted_hard_state.reset();
                } else if (choice < 55) {
                    std::vector<LogEntry> batch;
                    Term t = std::max(last_term, oracle.term);
                    for (Index i = last + 1, n = rng.between(1, 4); i <= last + n; ++i) {
                        batch.push_back({t, i, payload(rng)});
                    }
                    for (const LogEntry& e : batch) oracle.pending.emplace_back(std::in_place_type<LogEntry>, e);
                    storage->append(batch);
                } else if (choice < 65) {
                    // Raft only truncates uncommitted entries, never ones a snapshot covers.
                    const Index from = rng.between(snap_index + 1, last + 1);
                    if (from <= last) oracle.pending.emplace_back(std::in_place_type<TruncateFrom>, from);
                    storage->truncate_suffix(from);
                } else if (choice < 90) {
                    storage->sync();
                    oracle.synced = oracle.log();
                    oracle.pending.clear();
                } else {
                    // A snapshot of the log so far, or one from a leader that is further ahead.
                    const Index at = rng.between(snap_index + 1, last + 2);
                    const auto it = std::ranges::find(view, at, &LogEntry::index);
                    const Snapshot s{.last_included_index = at,
                                     .last_included_term = it != view.end() ? it->term : last_term + 1,
                                     .data = payload(rng)};
                    oracle.interrupted_snapshot = s;
                    oracle.interrupted_snapshot_log = after(view, s);
                    storage->save_snapshot(s);
                    oracle.snap = s;
                    oracle.synced = oracle.interrupted_snapshot_log;
                    oracle.pending.clear();
                    oracle.interrupted_snapshot.reset();
                }
                if (storage->load() != oracle.now()) {
                    return std::format("{}: FileStorage::load() is\n  {}\nbut should be\n  {}", where,
                                       describe(storage->load()), describe(oracle.now()));
                }
            }
        } catch (const PowerCut&) {
        }
        storage.reset();
        disk->crash(rng);
    }
    return std::nullopt;
}

TEST(PowerLoss, RecoveryKeepsEverythingDurableAndInventsNothing) {
    for (std::uint64_t seed : test::seeds(400)) {
        if (auto violation = run(seed, Bug::None)) FAIL() << *violation;
    }
}

// Postmortem 004: the power goes out after a snapshot past the end of the log is saved, but
// before the log is rewritten. Later entries must follow the snapshot on the next recovery.
TEST(PowerLoss, AnInterruptedCompactionIsFinishedOnRecovery) {
    sim::Rng rng(1);
    auto disk = std::make_shared<PowerLossIo>();
    auto open = [&] {
        auto s = FileStorage::open("/data", FileStorage::Sync::Durable, disk);
        if (!s) throw std::runtime_error(s.error());
        return std::move(*s);
    };
    {
        auto s = open();
        std::vector<LogEntry> log;
        for (Index i = 1; i <= 5; ++i) log.push_back({1, i, "x"});
        s->append(log);
        s->sync();
        // save_snapshot: write, sync, rename and sync_dir the snapshot; the power goes out on
        // the next operation, the start of the log rewrite.
        disk->cut_after(4);
        EXPECT_THROW(s->save_snapshot({.last_included_index = 7, .last_included_term = 1, .data = "s"}), PowerCut);
    }
    disk->crash(rng);
    {
        auto s = open();
        EXPECT_TRUE(s->load().log.empty());
        s->append(std::vector<LogEntry>{{1, 8, "y"}});
        s->sync();
    }
    disk->crash(rng);
    const auto state = open()->load();
    ASSERT_TRUE(state.snapshot);
    EXPECT_EQ(state.snapshot->last_included_index, 7u);
    EXPECT_EQ(state.log, (std::vector<LogEntry>{{1, 8, "y"}}));
}

// The test is only worth something if it notices a missing fsync. Each planted bug must be
// caught by some seed.
void expect_caught(Bug bug) {
    for (std::uint64_t seed = 1; seed <= 400; ++seed) {
        if (auto violation = run(seed, bug)) {
            SUCCEED() << *violation;
            return;
        }
    }
    ADD_FAILURE() << "400 seeds without noticing the missing fsync";
}

TEST(PowerLoss, CatchesAMissingDirectorySync) { expect_caught(Bug::NoDirSync); }
TEST(PowerLoss, CatchesRenamingAnUnsyncedFile) { expect_caught(Bug::NoTmpSync); }
TEST(PowerLoss, CatchesAMissingLogSync) { expect_caught(Bug::NoLogSync); }

}  // namespace
}  // namespace raftkv
