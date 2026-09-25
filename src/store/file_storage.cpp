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

// Replays log records into a state, checking that entries continue the log without gaps.
tl::expected<void, std::string> replay(const std::vector<LogRecord>& records, raft::PersistentState& state) {
    for (const LogRecord& r : records) {
        if (const auto* e = std::get_if<raft::LogEntry>(&r)) {
            if (e->index != state.log.size() + 1) {
                return tl::unexpected(std::format("log entry {} does not follow entry {}", e->index,
                                                  state.log.size()));
            }
            state.log.push_back(*e);
        } else {
            const raft::Index from = std::get<TruncateFrom>(r).index;
            if (from == 0 || from > state.log.size() + 1) {
                return tl::unexpected(std::format("truncate-from {} is outside a log of {} entries",
                                                  from, state.log.size()));
            }
            state.log.resize(static_cast<std::size_t>(from - 1));
        }
    }
    return {};
}

}  // namespace

tl::expected<std::unique_ptr<FileStorage>, std::string> FileStorage::open(const fs::path& dir, Sync sync) {
    std::error_code ec;
    fs::create_directories(dir, ec);
    if (ec) return tl::unexpected(std::format("create {}: {}", dir.string(), ec.message()));
    fs::remove(dir / kHardStateTmp, ec);   // a leftover from a crash mid-save; never renamed

    raft::PersistentState state;
    if (fs::exists(dir / kHardStateFile)) {
        auto bytes = read_file(dir / kHardStateFile);
        if (!bytes) return tl::unexpected(bytes.error());
        if (auto ok = decode_hard_state(*bytes, state); !ok) return tl::unexpected(ok.error());
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
    const fs::path tmp = dir_ / kHardStateTmp;
    const int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) fail("open " + tmp.string());
    try {
        write_all(fd, encode_hard_state(current_term, voted_for), tmp.string());
        sync_file(fd, tmp.string());
    } catch (...) {
        ::close(fd);
        throw;
    }
    ::close(fd);
    if (::rename(tmp.c_str(), (dir_ / kHardStateFile).c_str()) != 0) fail("rename " + tmp.string());
    sync_directory();   // makes the rename itself durable
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

void FileStorage::sync_file(int fd, const std::string& what) const {
    if (sync_mode_ == Sync::Durable) full_sync(fd, what);
}

void FileStorage::sync_directory() const {
    if (sync_mode_ == Sync::Durable) sync_dir(dir_);
}

void FileStorage::write_log(std::span<const std::byte> bytes) {
    write_all(log_fd_, bytes, (dir_ / kLogFile).string());
}

}  // namespace raftkv::store
