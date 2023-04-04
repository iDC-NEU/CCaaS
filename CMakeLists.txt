cmake_minimum_required(VERSION 3.14...3.22)
project(Taas)

# For fresh ubuntu 22.04 startup
# zlib1g-dev is for gzip compress
# libgoogle-perftools-dev for linking tcmalloc
# apt install git libtool make autoconf g++-11 zlib1g-dev libgoogle-perftools-dev libssl-dev
# apt install g++

# Set debug and release flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wno-unused-parameter -Wno-unused-variable")
SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${CMAKE_CXX_FLAGS_COMMON}  -g -O0 -fprofile-arcs -ftest-coverage")
SET(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} ${CMAKE_CXX_FLAGS_COMMON} -O2")

### CHECK g++ version
if(CMAKE_COMPILER_IS_GNUCXX)
    execute_process(COMMAND ${CMAKE_C_COMPILER} -dumpversion OUTPUT_VARIABLE GCC_VERSION)
    if (GCC_VERSION VERSION_GREATER 11.0 OR GCC_VERSION VERSION_EQUAL 11.0)
        message(STATUS "Support C++20.")
        set(CMAKE_CXX_STANDARD 20)
    else ()
        message(FATAL_ERROR "C++20 needed. Therefore a gcc compiler with a version higher than 11.0 is needed.")
    endif()
endif(CMAKE_COMPILER_IS_GNUCXX)

# ---- Include guards ----

if(PROJECT_SOURCE_DIR STREQUAL PROJECT_BINARY_DIR)
    message(
            FATAL_ERROR
            "In-source builds not allowed. Please make a new directory (called a build directory) and run CMake from there."
    )
endif()

# ---- Add dependencies via CPM ----
# see https://github.com/TheLartians/CPM.cmake for more info

include(vendor/CPM.cmake)

### Thread support
include(FindThreads)

# PackageProject.cmake will be used to make our target installable

CPMAddPackage("gh:TheLartians/PackageProject.cmake@1.8.0")

#standard input & output
CPMAddPackage(
        NAME fmt
        GITHUB_REPOSITORY fmtlib/fmt
        GIT_TAG 8.1.1
        OPTIONS "FMT_INSTALL YES" # create an installable target
        GIT_SHALLOW TRUE
)

CPMAddPackage(
        NAME yaml-cpp
        GITHUB_REPOSITORY jbeder/yaml-cpp
        VERSION 0.7.0
        GIT_TAG yaml-cpp-0.7.0
        OPTIONS "YAML_CPP_FORMAT_SOURCE OFF"
        GIT_SHALLOW TRUE
)

### Crypto support
find_package(openssl_install
        CONFIG
        REQUIRED
        PATHS ${PROJECT_SOURCE_DIR}/vendor
        )

### Gflags support
find_package(gflags_install
        CONFIG
        REQUIRED
        PATHS ${PROJECT_SOURCE_DIR}/vendor
        )

### Glog support
find_package(glog_install
        CONFIG
        REQUIRED
        PATHS ${PROJECT_SOURCE_DIR}/vendor
        )

### Add zmq support
find_package(zeromq_install
        CONFIG
        REQUIRED
        PATHS ${PROJECT_SOURCE_DIR}/vendor
        )

### Protobuf support
find_package(protobuf_install
        CONFIG
        REQUIRED
        PATHS ${PROJECT_SOURCE_DIR}/vendor
        )

### Gtest support
find_package(gtest_install
        CONFIG
        REQUIRED
        PATHS ${PROJECT_SOURCE_DIR}/vendor
        )

include(${CMAKE_CURRENT_SOURCE_DIR}/vendor/tools.cmake)

### compile protobuf
file(GLOB PROTO_FILES "${CMAKE_CURRENT_SOURCE_DIR}/include/proto/*.proto")
# https://stackoverflow.com/questions/26147454/how-to-automatically-move-pb-h-and-pb-h-under-include-and-src-directories
my_protobuf_generate_cpp(include/proto PROTO_SRCS PROTO_HDRS ${PROTO_FILES}) # this func is in tools.cmake
ADD_LIBRARY(${PROJECT_NAME}_proto ${PROTO_HDRS} ${PROTO_SRCS})

#add library tivk_client.a
if (NOT EXISTS "${CMAKE_BINARY_DIR}/lib/libtikv_client.a")
    message(STATUS "tar -xvf ${CMAKE_SOURCE_DIR}/libtikv_client.tar.xz")
    execute_process(
            COMMAND tar -xvf ${CMAKE_SOURCE_DIR}/libtikv_client.tar.xz
            WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}")
    execute_process(
            COMMAND mv "${CMAKE_SOURCE_DIR}/libtikv_client.a" "${CMAKE_BINARY_DIR}/lib"
            WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}")
endif()

# for linking tcmalloc_minimal
# apt-get install libgoogle-perftools-dev
find_library(gflags gflags PATHS ${PROJECT_BINARY_DIR}/lib)
#find_library(leveldb leveldb PATHS ${PROJECT_BINARY_DIR}/lib)

#include
include_directories(SYSTEM ${CMAKE_BINARY_DIR}/include)
LINK_DIRECTORIES(${CMAKE_BINARY_DIR}/lib)

include_directories("${CMAKE_SOURCE_DIR}/rust/tikv_client/include")

## ---- Add source files and Create project library----
## Note: globbing sources is considered bad practice as CMake's generators may not detect new files
## automatically. Keep that in mind when changing files, or explicitly mention them here.
file(GLOB_RECURSE headers CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/include/*.h")
file(GLOB_RECURSE sources CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")
# Note: for header-only libraries change all PUBLIC flags to INTERFACE and create an interface
# target: add_library(${PROJECT_NAME} INTERFACE)
add_library(${PROJECT_NAME} ${headers} ${sources} src/storage/tikv.cpp include/storage/tikv.h src/storage/redo_loger.cpp include/storage/redo_loger.h src/message/message.cpp include/message/message.h src/epoch/worker.cpp include/epoch/worker.h include/tools/zeromq.h src/storage/backup.cpp include/storage/backup.h)
set_target_properties(${PROJECT_NAME} PROPERTIES CXX_STANDARD 20)


# being a cross-platform target, we enforce standards conformance on MSVC
target_compile_options(${PROJECT_NAME} PUBLIC "$<$<COMPILE_LANG_AND_ID:CXX,MSVC>:/permissive->")

# NOTE: DO NOT LINK BRPC.SO AND BRAFT.SO TOGETHER
SET(COMMON_DEPENDENCIES
        glog
        yaml-cpp
        fmt
        pthread
        zmq
        protobuf
        tcmalloc
        ${PROJECT_NAME}_proto tikv_client dl ssl crypto
        )

# Link dependencies
# https://stackoverflow.com/questions/58997230/cmake-project-fails-to-find-shared-library
target_link_libraries(${PROJECT_NAME}
#        PRIVATE
        PUBLIC
        ${COMMON_DEPENDENCIES}
        "-Wl,--disable-new-dtags")

target_include_directories(
        ${PROJECT_NAME} PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/${PROJECT_NAME}-${PROJECT_VERSION}>
)

# ---- Create an installable target ----
# this allows users to install and find the library via `find_package()`.
# the location where the project's version header will be placed should match the project's regular
# header paths
string(TOLOWER ${PROJECT_NAME}/version.h VERSION_HEADER_LOCATION)
#add_subdirectory(Taas-Sharding)
#include_directories("${CMAKE_SOURCE_DIR}/rust/tikv_client/include")
#include_directories("${CMAKE_SOURCE_DIR}/include")
#add_subdirectory("src")


# ---- Create sharding executable ----

add_executable(${PROJECT_NAME}_sharding main.cpp)
set_target_properties(${PROJECT_NAME}_sharding PROPERTIES CXX_STANDARD 20 OUTPUT_NAME "Taas_sharding")
target_link_libraries(${PROJECT_NAME}_sharding Taas
        ${COMMON_DEPENDENCIES})

#target_link_libraries(Taas_sharding taas_core proto gflags cppzmq tikv_client pthread dl ssl crypto)
















##========================================
##            Library
##========================================
#include_directories("${CMAKE_SOURCE_DIR}/rust/tikv_client/include")
#include_directories("${CMAKE_SOURCE_DIR}/include")
#
#add_subdirectory("src")
#
#add_executable(Taas main.cpp)
#
#target_include_directories(Taas PUBLIC taas_core proto gflags cppzmq)
#
#target_link_directories(Taas PUBLIC "${CMAKE_BINARY_DIR}/lib")
#
#target_link_libraries(Taas taas_core proto gflags cppzmq tikv_client pthread dl ssl crypto)
