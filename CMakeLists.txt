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

add_library(raftkv
    src/version.cpp
    src/sim/sim.cpp
    src/sim/sim_storage.cpp
)
target_include_directories(raftkv PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(raftkv
    PUBLIC tl::expected
    PRIVATE raftkv_warnings
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
    )
    target_link_libraries(raftkv_tests PRIVATE raftkv raftkv_warnings GTest::gtest_main)
    gtest_discover_tests(raftkv_tests DISCOVERY_MODE PRE_TEST)
endif()

if(RAFTKV_BUILD_BENCH)
    find_package(benchmark CONFIG REQUIRED)

    add_executable(raftkv_bench bench/smoke_bench.cpp)
    target_link_libraries(raftkv_bench PRIVATE raftkv raftkv_warnings benchmark::benchmark_main)
endif()
