#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include <tl/expected.hpp>

// The file operations FileStorage needs, behind an interface: the real POSIX ones, or a model
// of a disk that loses power (tests/power_loss_io.hpp). Writes are not durable until sync();
// a new name from rename() is not durable until sync_dir() on its directory.
//
// Failures throw StorageFailure (store/file_storage.hpp), except read(), which reports a
// missing or unreadable file as an error value.
namespace raftkv::store {

class Io {
public:
    virtual ~Io() = default;

    virtual bool exists(const std::filesystem::path& p) = 0;
    virtual tl::expected<std::vector<std::byte>, std::string> read(const std::filesystem::path& p) = 0;
    virtual void create_dirs(const std::filesystem::path& dir) = 0;
    virtual void remove(const std::filesystem::path& p) = 0;   // a missing file is fine

    // Creates or empties `p`, then writes `bytes`.
    virtual void write_new(const std::filesystem::path& p, std::span<const std::byte> bytes) = 0;
    // Appends to `p`, creating it if needed.
    virtual void append(const std::filesystem::path& p, std::span<const std::byte> bytes) = 0;
    virtual void truncate(const std::filesystem::path& p, std::uint64_t size) = 0;

    // Makes `p`'s contents and size durable (fsync; F_FULLFSYNC on macOS).
    virtual void sync(const std::filesystem::path& p) = 0;
    virtual void rename(const std::filesystem::path& from, const std::filesystem::path& to) = 0;
    // Makes the names in `dir` durable, including any rename into it.
    virtual void sync_dir(const std::filesystem::path& dir) = 0;
};

// The real thing. Keeps a file open between appends, so appending to the log costs one
// write() call, as it did before this interface existed.
std::shared_ptr<Io> posix_io();

}  // namespace raftkv::store
