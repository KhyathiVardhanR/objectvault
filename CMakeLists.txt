cmake_minimum_required(VERSION 3.20)
project(objectvault CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
# Homebrew/vcpkg ship CMake configs; distro packages (e.g. Ubuntu) may only
# ship pkg-config files, so fall back to those.
find_package(Protobuf CONFIG QUIET)
if(NOT Protobuf_FOUND)
  find_package(Protobuf REQUIRED)
endif()
find_package(gRPC CONFIG QUIET)
find_package(PkgConfig REQUIRED)
if(NOT gRPC_FOUND)
  pkg_check_modules(GRPCPP REQUIRED IMPORTED_TARGET grpc++)
  add_library(gRPC::grpc++ ALIAS PkgConfig::GRPCPP)
  find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)
  add_executable(gRPC::grpc_cpp_plugin IMPORTED)
  set_target_properties(gRPC::grpc_cpp_plugin PROPERTIES IMPORTED_LOCATION ${GRPC_CPP_PLUGIN})
endif()
find_package(prometheus-cpp CONFIG REQUIRED)
find_package(Boost REQUIRED)
pkg_check_modules(PQXX REQUIRED IMPORTED_TARGET libpqxx)
pkg_check_modules(HIREDIS REQUIRED IMPORTED_TARGET hiredis)

# --- generated gRPC/protobuf code -------------------------------------------
set(GEN ${CMAKE_CURRENT_BINARY_DIR}/gen)
file(MAKE_DIRECTORY ${GEN})
# Explicit protoc invocation (portable across Homebrew and distro packages).
if(TARGET protobuf::protoc)
  set(PROTOC $<TARGET_FILE:protobuf::protoc>)
else()
  find_program(PROTOC protoc REQUIRED)
endif()
set(PROTO_SRCS ${GEN}/storage.pb.cc ${GEN}/storage.grpc.pb.cc)
set(PROTO_HDRS ${GEN}/storage.pb.h ${GEN}/storage.grpc.pb.h)
add_custom_command(
  OUTPUT ${PROTO_SRCS} ${PROTO_HDRS}
  COMMAND ${PROTOC} -I ${CMAKE_CURRENT_SOURCE_DIR}/proto
          --cpp_out=${GEN} --grpc_out=${GEN}
          --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
          ${CMAKE_CURRENT_SOURCE_DIR}/proto/storage.proto
  DEPENDS proto/storage.proto
  COMMENT "Generating gRPC/protobuf sources")
add_library(ov_proto STATIC ${PROTO_SRCS})
target_link_libraries(ov_proto PUBLIC protobuf::libprotobuf gRPC::grpc++)
target_include_directories(ov_proto PUBLIC ${GEN})

add_library(ov_common INTERFACE)
target_include_directories(ov_common INTERFACE src)
target_link_libraries(ov_common INTERFACE OpenSSL::Crypto Threads::Threads)

# --- storage node -------------------------------------------------------------
add_executable(ov-storage src/storage/main.cc)
target_link_libraries(ov-storage PRIVATE ov_proto ov_common prometheus-cpp::pull)

# --- gateway -------------------------------------------------------------------
add_executable(ov-gateway src/gateway/main.cc src/gateway/metadata.cc)
target_link_libraries(ov-gateway PRIVATE ov_proto ov_common prometheus-cpp::pull
  PkgConfig::PQXX PkgConfig::HIREDIS Boost::headers)

# --- benchmark tool -------------------------------------------------------------
add_executable(ov-bench tools/ov_bench.cc)
target_link_libraries(ov-bench PRIVATE Boost::headers Threads::Threads)
