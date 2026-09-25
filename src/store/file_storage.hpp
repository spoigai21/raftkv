#pragma once

#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>

#include <tl/expected.hpp>

#include "raft/storage.hpp"

namespace raftkv::store {

// Thrown when a write, sync or rename fails. The node must stop: after a failed fsync the
// kernel may already have dropped the dirty pages, so retrying cannot make the data durable.
class StorageFailure : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// Durable Storage on the local filesystem (implementation guide Phase 4). A data directory
// holds two files:
//
//   hard_state  current term and vote; replaced atomically (write tmp, fsync, rename, fsync dir)
//   log         append-only records (see log_codec.hpp): entries and truncate-from markers
//
// save_hard_state() is durable when it returns; append() and truncate_suffix() become
// durable at the next sync().
class FileStorage final : public raft::Storage {
public:
    enum class Sync {
        Durable,            // fsync (F_FULLFSYNC on macOS): survives power loss
        // Skips every fsync. Writes still reach the OS page cache, so they survive the
        // process being killed, but not the machine going down. For simulator tests, which
        // only ever crash processes and would otherwise spend nearly all their time in fsync.
        ProcessCrashOnly,
    };

    // Opens (creating if needed) the data directory and recovers its state. A torn log tail is
    // cut off; any other damage is an error and the node must not start.
    static tl::expected<std::unique_ptr<FileStorage>, std::string> open(
        const std::filesystem::path& dir, Sync sync = Sync::Durable);

    ~FileStorage() override;
    FileStorage(const FileStorage&) = delete;
    FileStorage& operator=(const FileStorage&) = delete;

    void save_hard_state(raft::Term current_term, std::optional<raft::NodeId> voted_for) override;
    void append(std::span<const raft::LogEntry> entries) override;
    void truncate_suffix(raft::Index from) override;
    void sync() override;
    raft::PersistentState load() const override { return state_; }

    // Whether open() found and cut off a torn tail.
    bool recovered_torn_tail() const { return recovered_torn_tail_; }

private:
    FileStorage(std::filesystem::path dir, int log_fd, raft::PersistentState state, bool torn, Sync sync);
    void write_log(std::span<const std::byte> bytes);
    void sync_file(int fd, const std::string& what) const;
    void sync_directory() const;

    std::filesystem::path dir_;
    Sync sync_mode_ = Sync::Durable;
    int log_fd_ = -1;
    raft::PersistentState state_;
    bool recovered_torn_tail_ = false;
};

}  // namespace raftkv::store
