#include "store/file_storage.hpp"

#include <gtest/gtest.h>

#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <fstream>
#include <iterator>

#include "sim/rng.hpp"
#include "store/log_codec.hpp"
#include "temp_dir.hpp"

namespace raftkv {
namespace {

namespace fs = std::filesystem;
using raft::LogEntry;
using store::FileStorage;
using test::TempDir;

std::unique_ptr<FileStorage> open_ok(const fs::path& dir) {
    auto s = FileStorage::open(dir);
    if (!s) ADD_FAILURE() << "open failed: " << s.error();
    return s ? std::move(*s) : nullptr;
}

std::vector<char> read_bytes(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    return {std::istreambuf_iterator<char>(in), {}};
}

void write_bytes(const fs::path& p, const std::vector<char>& bytes) {
    std::ofstream out(p, std::ios::binary | std::ios::trunc);
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
}

std::vector<LogEntry> entries(raft::Index from, raft::Index to, raft::Term term) {
    std::vector<LogEntry> out;
    for (raft::Index i = from; i <= to; ++i) out.push_back({term, i, "cmd" + std::to_string(i)});
    return out;
}

TEST(FileStorage, FreshDirectoryIsEmpty) {
    TempDir dir;
    auto s = open_ok(dir / "node");
    ASSERT_TRUE(s);
    EXPECT_EQ(s->load(), raft::PersistentState{});
}

TEST(FileStorage, ReopenRecoversEverything) {
    TempDir dir;
    raft::PersistentState want;
    {
        auto s = open_ok(dir.path());
        ASSERT_TRUE(s);
        s->save_hard_state(5, 2);
        s->append(entries(1, 4, 1));
        s->truncate_suffix(3);
        s->append(entries(3, 5, 5));
        s->sync();
        want = s->load();
    }
    ASSERT_EQ(want.log.size(), 5u);
    auto s = open_ok(dir.path());
    ASSERT_TRUE(s);
    EXPECT_EQ(s->load(), want);
    EXPECT_FALSE(s->recovered_torn_tail());
}

TEST(FileStorage, HardStateWithoutAVote) {
    TempDir dir;
    open_ok(dir.path())->save_hard_state(9, std::nullopt);
    auto s = open_ok(dir.path());
    EXPECT_EQ(s->load().current_term, 9u);
    EXPECT_FALSE(s->load().voted_for.has_value());
}

TEST(FileStorage, TornTailIsCutOffAndTheLogStaysUsable) {
    TempDir dir;
    {
        auto s = open_ok(dir.path());
        s->append(entries(1, 3, 1));
        s->sync();
    }
    // Chop 5 bytes off the last record, as if the crash hit mid-write.
    auto bytes = read_bytes(dir / "log");
    bytes.resize(bytes.size() - 5);
    write_bytes(dir / "log", bytes);
    {
        auto s = open_ok(dir.path());
        ASSERT_TRUE(s);
        EXPECT_TRUE(s->recovered_torn_tail());
        EXPECT_EQ(s->load().log, entries(1, 2, 1));
        s->append(entries(3, 4, 2));   // new records follow the valid prefix
        s->sync();
    }
    auto s = open_ok(dir.path());
    ASSERT_TRUE(s);
    EXPECT_FALSE(s->recovered_torn_tail());
    auto want = entries(1, 2, 1);
    for (auto& e : entries(3, 4, 2)) want.push_back(e);
    EXPECT_EQ(s->load().log, want);
}

TEST(FileStorage, CorruptRecordRefusesToOpen) {
    TempDir dir;
    {
        auto s = open_ok(dir.path());
        s->append(entries(1, 10, 1));
        s->sync();
    }
    auto bytes = read_bytes(dir / "log");
    bytes[bytes.size() / 2] ^= 0x10;
    write_bytes(dir / "log", bytes);
    auto s = FileStorage::open(dir.path());
    ASSERT_FALSE(s.has_value());
    EXPECT_NE(s.error().find("corrupt"), std::string::npos) << s.error();
}

TEST(FileStorage, CorruptHardStateRefusesToOpen) {
    TempDir dir;
    open_ok(dir.path())->save_hard_state(3, 1);
    auto bytes = read_bytes(dir / "hard_state");
    bytes[6] ^= 0x01;
    write_bytes(dir / "hard_state", bytes);
    auto s = FileStorage::open(dir.path());
    ASSERT_FALSE(s.has_value());
    EXPECT_NE(s.error().find("hard_state"), std::string::npos) << s.error();
}

TEST(FileStorage, LeftoverTempFileFromACrashedSaveIsIgnored) {
    TempDir dir;
    open_ok(dir.path())->save_hard_state(4, 1);
    write_bytes(dir / "hard_state.tmp", {'j', 'u', 'n', 'k'});
    auto s = open_ok(dir.path());
    ASSERT_TRUE(s);
    EXPECT_EQ(s->load().current_term, 4u);
    EXPECT_FALSE(fs::exists(dir / "hard_state.tmp"));
}

TEST(FileStorage, WellFormedRecordsThatSkipAnIndexAreRejected) {
    TempDir dir;
    std::vector<std::byte> bytes;
    store::encode(LogEntry{1, 1, "a"}, bytes);
    store::encode(LogEntry{1, 3, "c"}, bytes);   // no entry 2
    write_bytes(dir / "log", {reinterpret_cast<const char*>(bytes.data()),
                              reinterpret_cast<const char*>(bytes.data()) + bytes.size()});
    auto s = FileStorage::open(dir.path());
    ASSERT_FALSE(s.has_value());
    EXPECT_NE(s.error().find("does not follow"), std::string::npos) << s.error();
}

// The real thing: a child process appends and syncs, telling the parent about each entry
// only once sync() has returned. The parent SIGKILLs it at a random moment, which can land
// mid-write, then reopens the directory: nothing the child acknowledged may be missing.
TEST(FileStorage, KillNineNeverLosesAnAcknowledgedWrite) {
    sim::Rng rng(99);
    for (int round = 0; round < 8; ++round) {
        SCOPED_TRACE(testing::Message() << "round " << round);
        TempDir dir;
        int pipefd[2];
        ASSERT_EQ(::pipe(pipefd), 0);
        const pid_t pid = ::fork();
        ASSERT_GE(pid, 0);
        if (pid == 0) {
            ::close(pipefd[0]);
            auto s = FileStorage::open(dir.path());
            if (!s) ::_exit(2);
            for (raft::Index i = 1;; ++i) {
                LogEntry e{i / 10 + 1, i, "cmd" + std::to_string(i)};
                (*s)->append({&e, 1});
                (*s)->sync();
                if (i % 7 == 0) (*s)->save_hard_state(i, static_cast<raft::NodeId>(i % 3));
                if (::write(pipefd[1], &i, sizeof i) != sizeof i) ::_exit(3);
            }
        }
        ::close(pipefd[1]);
        const auto target = rng.between(20, 120);
        raft::Index acked = 0;
        raft::Index got = 0;
        while (acked < target && ::read(pipefd[0], &got, sizeof got) == sizeof got) acked = got;
        ::kill(pid, SIGKILL);
        int status = 0;
        ::waitpid(pid, &status, 0);
        ASSERT_TRUE(WIFSIGNALED(status)) << "child exited on its own with " << WEXITSTATUS(status);
        while (::read(pipefd[0], &got, sizeof got) == sizeof got) acked = got;   // sent before death
        ::close(pipefd[0]);

        auto s = FileStorage::open(dir.path());
        ASSERT_TRUE(s.has_value()) << s.error();
        const auto state = (*s)->load();
        ASSERT_GE(state.log.size(), acked) << "an acknowledged entry was lost";
        for (raft::Index i = 1; i <= state.log.size(); ++i) {
            const LogEntry want{i / 10 + 1, i, "cmd" + std::to_string(i)};
            ASSERT_EQ(state.log[i - 1], want) << "entry " << i;
        }
        EXPECT_GE(state.current_term, acked / 7 * 7) << "an acknowledged hard state was lost";
    }
}

}  // namespace
}  // namespace raftkv
