cmake_minimum_required(VERSION 3.20)
project(objectvault CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(prometheus-cpp CONFIG REQUIRED)
find_package(Boost REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(PQXX REQUIRED IMPORTED_TARGET libpqxx)
pkg_check_modules(HIREDIS REQUIRED IMPORTED_TARGET hiredis)

# --- generated gRPC/protobuf code -------------------------------------------
set(GEN ${CMAKE_CURRENT_BINARY_DIR}/gen)
file(MAKE_DIRECTORY ${GEN})
add_library(ov_proto STATIC proto/storage.proto)
target_link_libraries(ov_proto PUBLIC protobuf::libprotobuf gRPC::grpc++)
target_include_directories(ov_proto PUBLIC ${GEN})
protobuf_generate(TARGET ov_proto LANGUAGE cpp IMPORT_DIRS proto PROTOC_OUT_DIR ${GEN})
protobuf_generate(TARGET ov_proto LANGUAGE grpc IMPORT_DIRS proto PROTOC_OUT_DIR ${GEN}
  GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
  PLUGIN "protoc-gen-grpc=\$<TARGET_FILE:gRPC::grpc_cpp_plugin>")

add_library(ov_common INTERFACE)
target_include_directories(ov_common INTERFACE src)
target_link_libraries(ov_common INTERFACE OpenSSL::Crypto Threads::Threads)

# --- storage node -------------------------------------------------------------
add_executable(ov-storage src/storage/main.cc)
target_link_libraries(ov-storage PRIVATE ov_proto ov_common prometheus-cpp::pull)
