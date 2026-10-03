#include "store/file_storage.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <format>
#include <vector>

#include "store/log_codec.hpp"

namespace raftkv::store {

namespace fs = std::filesystem;

namespace {

constexpr const char* kLogFile = "log";
constexpr const char* kHardStateFile = "hard_state";
constexpr const char* kHardStateTmp = "hard_state.tmp";
constexpr const char* kSnapshotFile = "snapshot";
constexpr const char* kSnapshotTmp = "snapshot.tmp";
constexpr const char* kLogTmp = "log.tmp";
constexpr std::uint32_t kSnapshotMagic = 0x4e53'4b52;   // "RKSN"
constexpr std::uint32_t kHardStateMagic = 0x5348'4b52;   // "RKHS"
constexpr std::size_t kHardStateSize = 4 + 8 + 1 + 4 + 4;

[[noreturn]] void fail(const std::string& what) {
    throw StorageFailure(std::format("{}: {}", what, std::strerror(errno)));
}

// Makes everything written to `fd` durable. On macOS, fsync() only reaches the drive's
// cache; F_FULLFSYNC asks the drive to flush it (implementation guide Phase 4).
void full_sync(int fd, const std::string& what) {
#ifdef __APPLE__
    if (::fcntl(fd, F_FULLFSYNC) == 0) return;
    // Some filesystems (network, some virtual disks) reject F_FULLFSYNC; fall back.
#endif
    if (::fsync(fd) != 0) fail("fsync " + what);
}

void sync_dir(const fs::path& dir) {
    const int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) fail("open dir " + dir.string());
    full_sync(fd, dir.string());
    ::close(fd);
}

void write_all(int fd, std::span<const std::byte> bytes, const std::string& what) {
    while (!bytes.empty()) {
        const ssize_t n = ::write(fd, bytes.data(), bytes.size());
        if (n < 0) {
            if (errno == EINTR) continue;
            fail("write " + what);
        }
        bytes = bytes.subspan(static_cast<std::size_t>(n));
    }
}

tl::expected<std::vector<std::byte>, std::string> read_file(const fs::path& p) {
    const int fd = ::open(p.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) return tl::unexpected(std::format("open {}: {}", p.string(), std::strerror(errno)));
    std::vector<std::byte> out;
    std::byte buf[1 << 16];
    while (true) {
        const ssize_t n = ::read(fd, buf, sizeof buf);
        if (n < 0 && errno == EINTR) continue;
        if (n < 0) {
            const std::string err = std::format("read {}: {}", p.string(), std::strerror(errno));
            ::close(fd);
            return tl::unexpected(err);
        }
        if (n == 0) break;
        out.insert(out.end(), buf, buf + n);
    }
    ::close(fd);
    return out;
}

void put(std::vector<std::byte>& out, std::uint64_t v, int bytes) {
    for (int i = 0; i < bytes; ++i) out.push_back(static_cast<std::byte>(v >> (8 * i)));
}

std::uint64_t get(std::span<const std::byte> in, std::size_t at, int bytes) {
    std::uint64_t v = 0;
    for (int i = 0; i < bytes; ++i) {
        v |= std::to_integer<std::uint64_t>(in[at + static_cast<std::size_t>(i)]) << (8 * i);
    }
    return v;
}

// hard_state: [magic u32][term u64][has_vote u8][voted_for u32][crc32 of the preceding u32]
std::vector<std::byte> encode_hard_state(raft::Term term, std::optional<raft::NodeId> vote) {
    std::vector<std::byte> out;
    put(out, kHardStateMagic, 4);
    put(out, term, 8);
    put(out, vote ? 1 : 0, 1);
    put(out, vote.value_or(0), 4);
    put(out, crc32(out), 4);
    return out;
}

tl::expected<void, std::string> decode_hard_state(std::span<const std::byte> in,
                                                   raft::PersistentState& state) {
    if (in.size() != kHardStateSize || get(in, 0, 4) != kHardStateMagic ||
        crc32(in.first(kHardStateSize - 4)) != get(in, kHardStateSize - 4, 4)) {
        return tl::unexpected(std::string("hard_state is corrupt"));
    }
    const auto has_vote = get(in, 12, 1);
    if (has_vote > 1) return tl::unexpected(std::string("hard_state is corrupt"));
    state.current_term = get(in, 4, 8);
    state.voted_for = has_vote ? std::optional(static_cast<raft::NodeId>(get(in, 13, 4))) : std::nullopt;
    return {};
}

// Replays log records into a state. The log may start at index 1 or, once compacted, right
// after the snapshot; either way entries must follow each other without gaps. Entries the
// snapshot covers are dropped (they are there if a crash came between saving the snapshot
// and rewriting the log).
tl::expected<void, std::string> replay(const std::vector<LogRecord>& records, raft::PersistentState& state) {
    const raft::Index covered = state.snapshot ? state.snapshot->last_included_index : 0;
    std::vector<raft::LogEntry> log;
    std::optional<raft::Index> next;   // the index the next entry must have
    for (const LogRecord& r : records) {
        if (const auto* e = std::get_if<raft::LogEntry>(&r)) {
            if (!next) {
                if (e->index == 0 || e->index > covered + 1) {
                    return tl::unexpected(std::format("log starts at entry {}, after a gap (snapshot ends at {})",
                                                      e->index, covered));
                }
            } else if (e->index != *next) {
                return tl::unexpected(std::format("log entry {} does not follow entry {}", e->index, *next - 1));
            }
            log.push_back(*e);
            next = e->index + 1;
        } else {
            const raft::Index from = std::get<TruncateFrom>(r).index;
            const raft::Index first = log.empty() ? (next ? *next : 1) : log.front().index;
            if (from == 0 || from < first || (next && from > *next)) {
                return tl::unexpected(std::format("truncate-from {} is outside the log", from));
            }
            std::erase_if(log, [from](const raft::LogEntry& x) { return x.index >= from; });
            next = from;
        }
    }
    std::erase_if(log, [covered](const raft::LogEntry& x) { return x.index <= covered; });
    state.log = std::move(log);
    return {};
}

// snapshot: [magic u32][last_included_index u64][last_included_term u64][data length u64]
//           [data][crc32 of all the preceding bytes u32]
std::vector<std::byte> encode_snapshot(const raft::Snapshot& snap) {
    std::vector<std::byte> out;
    put(out, kSnapshotMagic, 4);
    put(out, snap.last_included_index, 8);
    put(out, snap.last_included_term, 8);
    put(out, snap.data.size(), 8);
    const auto data = std::as_bytes(std::span(snap.data));
    out.insert(out.end(), data.begin(), data.end());
    put(out, crc32(out), 4);
    return out;
}

tl::expected<raft::Snapshot, std::string> decode_snapshot(std::span<const std::byte> in) {
    constexpr std::size_t kFixed = 4 + 8 + 8 + 8;
    if (in.size() < kFixed + 4 || get(in, 0, 4) != kSnapshotMagic) {
        return tl::unexpected(std::string("snapshot is corrupt"));
    }
    const std::uint64_t len = get(in, 20, 8);
    if (len != in.size() - kFixed - 4 || crc32(in.first(in.size() - 4)) != get(in, in.size() - 4, 4)) {
        return tl::unexpected(std::string("snapshot is corrupt"));
    }
    const auto data = in.subspan(kFixed, static_cast<std::size_t>(len));
    return raft::Snapshot{.last_included_index = get(in, 4, 8), .last_included_term = get(in, 12, 8),
                          .data = std::string(reinterpret_cast<const char*>(data.data()), data.size())};
}

// Writes `bytes` to dir/tmp_name, makes it durable, and renames it over dir/name.
void replace_file(const fs::path& dir, const char* name, const char* tmp_name, std::span<const std::byte> bytes,
                  bool durable) {
    const fs::path tmp = dir / tmp_name;
    const int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) fail("open " + tmp.string());
    try {
        write_all(fd, bytes, tmp.string());
        if (durable) full_sync(fd, tmp.string());
    } catch (...) {
        ::close(fd);
        throw;
    }
    ::close(fd);
    if (::rename(tmp.c_str(), (dir / name).c_str()) != 0) fail("rename " + tmp.string());
    if (durable) sync_dir(dir);   // makes the rename itself durable
}

}  // namespace

tl::expected<std::unique_ptr<FileStorage>, std::string> FileStorage::open(const fs::path& dir, Sync sync) {
    std::error_code ec;
    fs::create_directories(dir, ec);
    if (ec) return tl::unexpected(std::format("create {}: {}", dir.string(), ec.message()));
    // Leftovers from a crash mid-save, never renamed into place.
    for (const char* tmp : {kHardStateTmp, kSnapshotTmp, kLogTmp}) fs::remove(dir / tmp, ec);

    raft::PersistentState state;
    if (fs::exists(dir / kHardStateFile)) {
        auto bytes = read_file(dir / kHardStateFile);
        if (!bytes) return tl::unexpected(bytes.error());
        if (auto ok = decode_hard_state(*bytes, state); !ok) return tl::unexpected(ok.error());
    }

    if (fs::exists(dir / kSnapshotFile)) {
        auto bytes = read_file(dir / kSnapshotFile);
        if (!bytes) return tl::unexpected(bytes.error());
        auto snap = decode_snapshot(*bytes);
        if (!snap) return tl::unexpected(snap.error());
        state.snapshot = std::move(*snap);
    }

    const fs::path log_path = dir / kLogFile;
    const int fd = ::open(log_path.c_str(), O_RDWR | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
    if (fd < 0) return tl::unexpected(std::format("open {}: {}", log_path.string(), std::strerror(errno)));
    auto close_on_error = [fd](std::string err) {
        ::close(fd);
        return tl::unexpected(std::move(err));
    };

    auto bytes = read_file(log_path);
    if (!bytes) return close_on_error(bytes.error());
    auto scanned = scan(*bytes);
    if (!scanned) {
        return close_on_error(std::format("{}: corrupt at or after a valid prefix: {}",
                                          log_path.string(), to_string(scanned.error())));
    }
    if (auto ok = replay(scanned->records, state); !ok) return close_on_error(ok.error());

    if (scanned->torn_tail) {
        // Cut the torn tail off durably, so new records follow the last valid one.
        if (::ftruncate(fd, static_cast<off_t>(scanned->valid_bytes)) != 0) {
            return close_on_error(std::format("truncate {}: {}", log_path.string(), std::strerror(errno)));
        }
        try {
            if (sync == Sync::Durable) full_sync(fd, log_path.string());
        } catch (const StorageFailure& e) {
            return close_on_error(e.what());
        }
    }
    return std::unique_ptr<FileStorage>(
        new FileStorage(dir, fd, std::move(state), scanned->torn_tail, sync));
}

FileStorage::FileStorage(fs::path dir, int log_fd, raft::PersistentState state, bool torn, Sync sync)
    : dir_(std::move(dir)),
      sync_mode_(sync),
      log_fd_(log_fd),
      state_(std::move(state)),
      recovered_torn_tail_(torn) {}

FileStorage::~FileStorage() {
    if (log_fd_ >= 0) ::close(log_fd_);
}

void FileStorage::save_hard_state(raft::Term current_term, std::optional<raft::NodeId> voted_for) {
    replace_file(dir_, kHardStateFile, kHardStateTmp, encode_hard_state(current_term, voted_for),
                 sync_mode_ == Sync::Durable);
    state_.current_term = current_term;
    state_.voted_for = voted_for;
}

void FileStorage::append(std::span<const raft::LogEntry> entries) {
    std::vector<std::byte> bytes;
    for (const raft::LogEntry& e : entries) encode(e, bytes);
    write_log(bytes);
    state_.log.insert(state_.log.end(), entries.begin(), entries.end());
}

void FileStorage::truncate_suffix(raft::Index from) {
    from = std::max<raft::Index>(from, 1);   // index 0 is the sentinel, never stored
    if (from > state_.log.size()) return;    // nothing to delete
    std::vector<std::byte> bytes;
    encode(TruncateFrom{from}, bytes);
    write_log(bytes);
    state_.log.resize(static_cast<std::size_t>(from - 1));
}

void FileStorage::sync() { sync_file(log_fd_, (dir_ / kLogFile).string()); }

void FileStorage::save_snapshot(const raft::Snapshot& snapshot) {
    const bool durable = sync_mode_ == Sync::Durable;
    // 1. The snapshot, durably. From here a crash recovers the snapshot plus the old log,
    //    whose covered entries replay() drops.
    replace_file(dir_, kSnapshotFile, kSnapshotTmp, encode_snapshot(snapshot), durable);
    state_.snapshot = snapshot;
    std::erase_if(state_.log, [&](const raft::LogEntry& e) { return e.index <= snapshot.last_included_index; });

    // 2. Rewrite the log with only the entries after the snapshot, and swap it in. This is
    //    what keeps the log bounded.
    std::vector<std::byte> bytes;
    for (const raft::LogEntry& e : state_.log) encode(e, bytes);
    replace_file(dir_, kLogFile, kLogTmp, bytes, durable);
    const fs::path log_path = dir_ / kLogFile;
    const int fd = ::open(log_path.c_str(), O_RDWR | O_APPEND | O_CLOEXEC);
    if (fd < 0) fail("reopen " + log_path.string());
    ::close(log_fd_);
    log_fd_ = fd;
}

void FileStorage::sync_file(int fd, const std::string& what) const {
    if (sync_mode_ == Sync::Durable) full_sync(fd, what);
}

void FileStorage::write_log(std::span<const std::byte> bytes) {
    write_all(log_fd_, bytes, (dir_ / kLogFile).string());
}

}  // namespace raftkv::store
