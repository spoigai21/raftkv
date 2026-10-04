#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <format>
#include <map>

#include "store/file_storage.hpp"
#include "store/io.hpp"

namespace raftkv::store {

namespace fs = std::filesystem;

namespace {

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

class PosixIo final : public Io {
public:
    ~PosixIo() override {
        for (auto& [path, fd] : open_) ::close(fd);
    }

    bool exists(const fs::path& p) override { return fs::exists(p); }

    tl::expected<std::vector<std::byte>, std::string> read(const fs::path& p) override {
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

    void create_dirs(const fs::path& dir) override {
        std::error_code ec;
        fs::create_directories(dir, ec);
        if (ec) throw StorageFailure(std::format("create {}: {}", dir.string(), ec.message()));
    }

    void remove(const fs::path& p) override {
        forget(p);
        std::error_code ec;
        fs::remove(p, ec);
    }

    void write_new(const fs::path& p, std::span<const std::byte> bytes) override {
        forget(p);
        const int fd = ::open(p.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
        if (fd < 0) fail("open " + p.string());
        open_[p.string()] = fd;   // kept open for the sync() that normally follows
        write_all(fd, bytes, p.string());
    }

    void append(const fs::path& p, std::span<const std::byte> bytes) override {
        write_all(fd_for_append(p), bytes, p.string());
    }

    void truncate(const fs::path& p, std::uint64_t size) override {
        if (::truncate(p.c_str(), static_cast<off_t>(size)) != 0) fail("truncate " + p.string());
    }

    void sync(const fs::path& p) override {
        if (auto it = open_.find(p.string()); it != open_.end()) {
            full_sync(it->second, p.string());
            return;
        }
        const int fd = ::open(p.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd < 0) fail("open " + p.string());
        full_sync(fd, p.string());
        ::close(fd);
    }

    void rename(const fs::path& from, const fs::path& to) override {
        // Both names now refer to different files than any descriptor we hold for them.
        forget(from);
        forget(to);
        if (::rename(from.c_str(), to.c_str()) != 0) fail("rename " + from.string());
    }

    void sync_dir(const fs::path& dir) override {
        const int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (fd < 0) fail("open dir " + dir.string());
        full_sync(fd, dir.string());
        ::close(fd);
    }

private:
    int fd_for_append(const fs::path& p) {
        // Files are opened for appending here, so a descriptor from write_new (opened without
        // O_APPEND) is replaced with one that appends.
        auto it = appending_.find(p.string());
        if (it != appending_.end()) return it->second;
        forget(p);
        const int fd = ::open(p.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
        if (fd < 0) fail("open " + p.string());
        open_[p.string()] = fd;
        appending_[p.string()] = fd;
        return fd;
    }

    void forget(const fs::path& p) {
        if (auto it = open_.find(p.string()); it != open_.end()) {
            ::close(it->second);
            open_.erase(it);
        }
        appending_.erase(p.string());
    }

    std::map<std::string, int> open_;        // descriptors we hold, by path
    std::map<std::string, int> appending_;   // the subset opened with O_APPEND
};

}  // namespace

std::shared_ptr<Io> posix_io() { return std::make_shared<PosixIo>(); }

}  // namespace raftkv::store
