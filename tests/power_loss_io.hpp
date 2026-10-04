#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <format>
#include <map>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <vector>

#include "sim/rng.hpp"
#include "store/file_storage.hpp"
#include "store/io.hpp"

// A model of a disk that loses power, for testing that FileStorage calls fsync in the right
// places. It follows the POSIX rules a careful program relies on, and nothing kinder:
//
//   - A file's contents are durable only up to its last sync(). After a power cut, an
//     unsynced append keeps any prefix of its bytes (possibly none); an unsynced truncate
//     may or may not have happened.
//   - A name (from creating, renaming or removing a file) is durable only after sync_dir()
//     on its directory. Syncing a file does not make its name durable. After a power cut,
//     each name independently points at either its old file or its new one.
//
// This tests the order of our writes and syncs. It cannot test whether a real drive honors
// them: a drive that acknowledges a flush it has not done defeats any software.
namespace raftkv::test {

// Thrown by the operation the power went out on, which does not happen.
struct PowerCut {};

class PowerLossIo final : public store::Io {
public:
    // The power goes out on the `ops`-th file operation from now (0: the very next one).
    void cut_after(std::uint64_t ops) { ops_left_ = ops; }
    void no_cut() { ops_left_.reset(); }
    std::uint64_t ops() const { return ops_; }

    // The power has gone out: pick what survived, then carry on as a disk that just booted.
    void crash(sim::Rng& rng) {
        no_cut();
        std::set<std::string> paths;
        for (const auto& [p, _] : names_) paths.insert(p);
        for (const auto& [p, _] : durable_names_) paths.insert(p);
        std::map<std::string, std::size_t> names;
        for (const std::string& p : paths) {
            const auto now = find(names_, p);
            const auto before = find(durable_names_, p);
            const auto pick = (now == before || rng.chance(0.5)) ? before : now;
            if (pick) names[p] = *pick;
        }
        std::set<std::size_t> survivors;
        for (const auto& [_, ino] : names) survivors.insert(ino);
        for (std::size_t ino : survivors) {
            Inode& f = inodes_[ino];
            if (f.current != f.durable) {
                if (rng.chance(0.25)) {
                    f.current = f.durable;   // nothing since the last sync landed
                } else {
                    // Appends are kept up to some byte; an unsynced truncate may have landed.
                    f.current.resize(rng.between(f.floor, f.current.size()));
                }
            }
            f.durable = f.current;
            f.floor = f.current.size();
        }
        names_ = names;
        durable_names_ = std::move(names);
    }

    bool exists(const std::filesystem::path& p) override { return names_.contains(p.string()); }

    tl::expected<std::vector<std::byte>, std::string> read(const std::filesystem::path& p) override {
        const auto ino = find(names_, p.string());
        if (!ino) return tl::unexpected(std::format("read {}: no such file", p.string()));
        return inodes_[*ino].current;
    }

    void create_dirs(const std::filesystem::path&) override { step(); }

    void remove(const std::filesystem::path& p) override {
        step();
        names_.erase(p.string());
    }

    void write_new(const std::filesystem::path& p, std::span<const std::byte> bytes) override {
        step();
        names_[p.string()] = new_inode();
        inodes_.back().current.assign(bytes.begin(), bytes.end());
    }

    void append(const std::filesystem::path& p, std::span<const std::byte> bytes) override {
        step();
        auto ino = find(names_, p.string());
        if (!ino) ino = names_[p.string()] = new_inode();
        auto& f = inodes_[*ino].current;
        f.insert(f.end(), bytes.begin(), bytes.end());
    }

    void truncate(const std::filesystem::path& p, std::uint64_t size) override {
        step();
        Inode& f = inodes_[at(p)];
        f.current.resize(std::min<std::size_t>(size, f.current.size()));
        f.floor = std::min(f.floor, f.current.size());
    }

    void sync(const std::filesystem::path& p) override {
        step();
        Inode& f = inodes_[at(p)];
        f.durable = f.current;
        f.floor = f.current.size();
    }

    void rename(const std::filesystem::path& from, const std::filesystem::path& to) override {
        step();
        names_[to.string()] = at(from);
        names_.erase(from.string());
    }

    void sync_dir(const std::filesystem::path& dir) override {
        step();
        std::erase_if(durable_names_, [&](const auto& kv) { return std::filesystem::path(kv.first).parent_path() == dir; });
        for (const auto& [p, ino] : names_) {
            if (std::filesystem::path(p).parent_path() == dir) durable_names_[p] = ino;
        }
    }

private:
    struct Inode {
        std::vector<std::byte> durable;   // what survives a power cut for certain
        std::vector<std::byte> current;   // what reads see
        std::size_t floor = 0;            // the shortest the file has been since the last sync
    };

    static std::optional<std::size_t> find(const std::map<std::string, std::size_t>& names, const std::string& p) {
        auto it = names.find(p);
        if (it == names.end()) return std::nullopt;
        return it->second;
    }

    std::size_t at(const std::filesystem::path& p) const {
        auto ino = find(names_, p.string());
        if (!ino) throw store::StorageFailure(std::format("{}: no such file", p.string()));
        return *ino;
    }

    std::size_t new_inode() {
        inodes_.emplace_back();
        return inodes_.size() - 1;
    }

    void step() {
        ++ops_;
        if (ops_left_ && (*ops_left_)-- == 0) {
            ops_left_.reset();
            throw PowerCut{};
        }
    }

    std::vector<Inode> inodes_;
    std::map<std::string, std::size_t> names_;           // what the program sees
    std::map<std::string, std::size_t> durable_names_;   // what survives a power cut for certain
    std::optional<std::uint64_t> ops_left_;
    std::uint64_t ops_ = 0;
};

}  // namespace raftkv::test
