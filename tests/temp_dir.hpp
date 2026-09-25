#pragma once

#include <stdlib.h>

#include <filesystem>
#include <stdexcept>
#include <string>

namespace raftkv::test {

// A fresh directory under the system temp dir, removed with everything in it on destruction.
class TempDir {
public:
    TempDir() {
        std::string tmpl = (std::filesystem::temp_directory_path() / "raftkv-test-XXXXXX").string();
        if (::mkdtemp(tmpl.data()) == nullptr) throw std::runtime_error("mkdtemp failed");
        path_ = tmpl;
    }
    ~TempDir() {
        std::error_code ec;
        std::filesystem::remove_all(path_, ec);
    }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;

    const std::filesystem::path& path() const { return path_; }
    std::filesystem::path operator/(const std::string& name) const { return path_ / name; }

private:
    std::filesystem::path path_;
};

}  // namespace raftkv::test
