cmake_minimum_required(VERSION 3.16)
project(matching_engine LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "" FORCE)
endif()

option(ME_BUILD_TESTS "Build unit/differential tests" ON)
option(ME_BUILD_BENCHMARKS "Build benchmark executables" ON)
option(ME_BUILD_GRPC "Build gRPC server/clients (requires gRPC + protobuf)" ON)
option(ME_NATIVE "Compile with -march=native (benchmark builds)" OFF)
option(ME_FRAME_POINTERS "Keep frame pointers for perf/flamegraphs" OFF)
set(ME_SANITIZE "" CACHE STRING "Sanitizer: address, undefined, thread, or address,undefined")

add_compile_options(-Wall -Wextra -Wpedantic)
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O3 -g -DNDEBUG")
if(ME_NATIVE)
  add_compile_options(-march=native)
endif()
if(ME_FRAME_POINTERS)
  add_compile_options(-fno-omit-frame-pointer)
endif()
if(ME_SANITIZE)
  add_compile_options(-fsanitize=${ME_SANITIZE} -fno-omit-frame-pointer -g -O1)
  add_link_options(-fsanitize=${ME_SANITIZE})
endif()

find_package(Threads REQUIRED)

# ---------------------------------------------------------------- engine ----
add_library(me_engine STATIC src/engine/types.cpp)
target_include_directories(me_engine PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(me_engine PUBLIC Threads::Threads)

# --------------------------------------------------------------- metrics ----
add_library(me_metrics STATIC src/metrics/metrics.cpp src/metrics/http_exporter.cpp)
target_link_libraries(me_metrics PUBLIC me_engine)

# ----------------------------------------------------------------- tests ----
if(ME_BUILD_TESTS)
  enable_testing()
  find_package(GTest REQUIRED)
  add_executable(me_tests
    tests/test_order_book.cpp
    tests/test_level_index.cpp
    tests/test_order_index.cpp
    tests/test_engine.cpp
    tests/test_differential.cpp
    tests/test_sharded_engine.cpp
    tests/test_metrics.cpp)
  target_link_libraries(me_tests PRIVATE me_engine me_metrics GTest::gtest GTest::gtest_main)
  add_test(NAME me_tests COMMAND me_tests)
endif()

# ------------------------------------------------------------ benchmarks ----
if(ME_BUILD_BENCHMARKS)
  add_executable(me_bench benchmarks/bench_main.cpp)
  target_link_libraries(me_bench PRIVATE me_engine me_metrics)
  target_compile_options(me_bench PRIVATE $<$<CXX_COMPILER_ID:GNU>:-Wno-mismatched-new-delete>)
  string(TOUPPER "${CMAKE_BUILD_TYPE}" ME_BUILD_TYPE_UPPER)
  target_compile_definitions(me_bench PRIVATE ME_CXX_FLAGS="${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${ME_BUILD_TYPE_UPPER}}$<$<BOOL:${ME_NATIVE}>: -march=native>")
  find_package(benchmark QUIET)
  if(benchmark_FOUND)
    add_executable(me_microbench benchmarks/microbench.cpp)
    target_link_libraries(me_microbench PRIVATE me_engine benchmark::benchmark)
  endif()
endif()

# ------------------------------------------------------------------ gRPC ----
if(ME_BUILD_GRPC)
  add_subdirectory(src/grpc)
endif()
