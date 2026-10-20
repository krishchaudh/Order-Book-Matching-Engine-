# gRPC service, example client and load tester. Uses pkg-config because the
# Ubuntu libgrpc++-dev package ships no CMake config files.
find_package(PkgConfig REQUIRED)
pkg_check_modules(GRPCPP REQUIRED IMPORTED_TARGET grpc++)
pkg_check_modules(PROTOBUF REQUIRED IMPORTED_TARGET protobuf)
find_program(PROTOC protoc REQUIRED)
find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)

set(PROTO_SRC ${CMAKE_SOURCE_DIR}/proto/matching_engine.proto)
set(GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/gen)
file(MAKE_DIRECTORY ${GEN_DIR})
set(GEN_SRCS
  ${GEN_DIR}/matching_engine.pb.cc
  ${GEN_DIR}/matching_engine.grpc.pb.cc)
add_custom_command(
  OUTPUT ${GEN_SRCS} ${GEN_DIR}/matching_engine.pb.h ${GEN_DIR}/matching_engine.grpc.pb.h
  COMMAND ${PROTOC} -I ${CMAKE_SOURCE_DIR}/proto --cpp_out=${GEN_DIR} --grpc_out=${GEN_DIR}
          --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN} ${PROTO_SRC}
  DEPENDS ${PROTO_SRC}
  COMMENT "protoc matching_engine.proto")

add_library(me_proto STATIC ${GEN_SRCS})
target_include_directories(me_proto PUBLIC ${GEN_DIR})
target_link_libraries(me_proto PUBLIC PkgConfig::GRPCPP PkgConfig::PROTOBUF)
target_compile_options(me_proto PRIVATE -w)

add_executable(me_server server.cpp)
target_link_libraries(me_server PRIVATE me_engine me_metrics me_proto)

add_executable(me_client client.cpp)
target_link_libraries(me_client PRIVATE me_proto)

add_executable(me_loadtest loadtest.cpp)
target_link_libraries(me_loadtest PRIVATE me_proto)

install(TARGETS me_server me_client me_loadtest RUNTIME DESTINATION bin)
