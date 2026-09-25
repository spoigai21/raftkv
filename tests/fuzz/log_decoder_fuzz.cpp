// libFuzzer target for the on-disk log format (implementation guide Phase 4).
//
// Feeds arbitrary bytes to decode() and scan(). ASan/UBSan catch memory errors; the checks
// below catch logic errors: whatever decodes must re-encode to exactly the bytes it came from.
//
//   cmake --preset fuzz && cmake --build --preset fuzz --target log_decoder_fuzz
//   ./build/fuzz/log_decoder_fuzz -max_total_time=60 <corpus dir>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "store/log_codec.hpp"

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
    using namespace raftkv::store;
    const auto in = std::as_bytes(std::span(data, size));

    if (auto d = decode(in)) {
        std::vector<std::byte> again;
        encode(d->record, again);
        if (again.size() != d->size || !std::equal(again.begin(), again.end(), in.begin())) {
            __builtin_trap();
        }
    }

    if (auto s = scan(in)) {
        std::vector<std::byte> again;
        for (const LogRecord& r : s->records) encode(r, again);
        if (again.size() != s->valid_bytes || !std::equal(again.begin(), again.end(), in.begin())) {
            __builtin_trap();
        }
    }
    return 0;
}
