cmake_minimum_required(VERSION 3.25)
project(raftkv VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(RAFTKV_BUILD_TESTS "Build unit tests" ON)
option(RAFTKV_BUILD_BENCH "Build benchmarks" ON)
option(RAFTKV_FUZZ "Build libFuzzer targets (clang only; targets arrive in Phase 4)" OFF)

# Every dependency is found now, even ones Phase 0 does not use yet, so that vcpkg
# problems (protobuf/abseil above all) surface before there is any logic to debug.
find_package(asio CONFIG REQUIRED)
find_package(Protobuf CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(tl-expected CONFIG REQUIRED)

# Warnings apply to raftkv targets only: not to vcpkg ports, and not to generated protobuf code.
add_library(raftkv_warnings INTERFACE)
target_compile_options(raftkv_warnings INTERFACE -Wall -Wextra -Wpedantic -Werror)
# GCC's TSan warns that it does not model std::atomic_thread_fence, which Asio uses
# internally, and -Werror makes that fatal. Our processes are single-threaded (implementation
# guide, Phase 5), so Asio's fences never synchronize anything TSan would need to see.
# Clang has no such warning; GCC ignores this flag without -fsanitize=thread.
target_compile_options(raftkv_warnings INTERFACE $<$<CXX_COMPILER_ID:GNU>:-Wno-tsan>)

# Generated protobuf code: its own library, without raftkv_warnings, and its headers are
# SYSTEM so they never trip -Werror in the code that includes them.
set(RAFTKV_PROTO_OUT ${CMAKE_CURRENT_BINARY_DIR}/proto)
file(MAKE_DIRECTORY ${RAFTKV_PROTO_OUT})
add_library(raftkv_proto STATIC src/raft/messages.proto src/kv/kv.proto src/net/net.proto)
target_link_libraries(raftkv_proto PUBLIC protobuf::libprotobuf-lite)
target_include_directories(raftkv_proto SYSTEM PUBLIC ${RAFTKV_PROTO_OUT})
protobuf_generate(
    TARGET raftkv_proto
    LANGUAGE cpp
    IMPORT_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/src
    PROTOC_OUT_DIR ${RAFTKV_PROTO_OUT}
)

add_library(raftkv
    src/version.cpp
    src/kv/client.cpp
    src/kv/messages.cpp
    src/kv/server.cpp
    src/kv/state_machine.cpp
    src/net/asio_env.cpp
    src/net/cluster_config.cpp
    src/net/frame.cpp
    src/raft/messages.cpp
    src/raft/raft.cpp
    src/sim/sim.cpp
    src/sim/sim_storage.cpp
    src/store/file_storage.cpp
    src/store/log_codec.cpp
)
target_include_directories(raftkv PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(raftkv
    PUBLIC tl::expected asio::asio
    PRIVATE raftkv_proto raftkv_warnings nlohmann_json::nlohmann_json
)
target_compile_definitions(raftkv PRIVATE RAFTKV_VERSION="${PROJECT_VERSION}")

add_executable(raftkvd apps/raftkvd.cpp)
target_link_libraries(raftkvd PRIVATE raftkv raftkv_warnings)

add_executable(raftkvctl apps/raftkvctl.cpp)
target_link_libraries(raftkvctl PRIVATE raftkv raftkv_warnings)

if(RAFTKV_BUILD_TESTS)
    enable_testing()
    find_package(GTest CONFIG REQUIRED)
    include(GoogleTest)

    add_executable(raftkv_tests
        tests/smoke_test.cpp
        tests/sim_storage_test.cpp
        tests/sim_test.cpp
        tests/raft_election_test.cpp
        tests/raft_replication_test.cpp
        tests/log_codec_test.cpp
        tests/file_storage_test.cpp
        tests/raft_persistence_test.cpp
        tests/kv_state_machine_test.cpp
        tests/kv_test.cpp
        tests/net_test.cpp
    )
    target_link_libraries(raftkv_tests PRIVATE raftkv raftkv_warnings GTest::gtest_main)
    gtest_discover_tests(raftkv_tests DISCOVERY_MODE PRE_TEST)

    # Real processes: three raftkvd and raftkvctl on localhost (tests/cluster/smoke.sh).
    add_test(NAME cluster_smoke
             COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/tests/cluster/smoke.sh
                     $<TARGET_FILE:raftkvd> $<TARGET_FILE:raftkvctl>)
    set_tests_properties(cluster_smoke PROPERTIES TIMEOUT 180)
endif()

if(RAFTKV_FUZZ)
    add_executable(log_decoder_fuzz tests/fuzz/log_decoder_fuzz.cpp)
    target_link_libraries(log_decoder_fuzz PRIVATE raftkv raftkv_warnings)
    target_link_options(log_decoder_fuzz PRIVATE -fsanitize=fuzzer)
endif()

if(RAFTKV_BUILD_BENCH)
    find_package(benchmark CONFIG REQUIRED)

    add_executable(raftkv_bench bench/smoke_bench.cpp)
    target_link_libraries(raftkv_bench PRIVATE raftkv raftkv_warnings benchmark::benchmark_main)
endif()
