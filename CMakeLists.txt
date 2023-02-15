cmake_minimum_required(VERSION 3.22)
project(Taas)

set(CMAKE_CXX_STANDARD 17)

#========================================
#               Dependencies
#========================================

set(DEPENDENCIES_DIR "${CMAKE_SOURCE_DIR}/.deps")

if (EXISTS "${DEPENDENCIES_DIR}" AND IS_DIRECTORY "${DEPENDENCIES_DIR}" AND NOT ${FETCH_DEPENDENCIES})
    message(STATUS "Using dependencies from ${DEPENDENCIES_DIR}")

    list(APPEND CMAKE_PREFIX_PATH ${DEPENDENCIES_DIR})
    # gflags does not include this in its target so need to add manually
    link_directories("${DEPENDENCIES_DIR}/lib")

    set(GFLAGS_USE_TARGET_NAMESPACE TRUE)
    find_package(gflags REQUIRED)
    find_package(glog REQUIRED)

    find_package(cppzmq REQUIRED)

    find_package(RapidJSON REQUIRED)

    add_library(rapidjson INTERFACE)
    target_include_directories(rapidjson INTERFACE "${RAPIDJSON_INCLUDE_DIRS}")

else()
    message(STATUS "Fetching dependencies")

    include(FetchContent)

    FetchContent_Declare(libzmq
            GIT_REPOSITORY  https://github.com/zeromq/libzmq.git
            GIT_TAG         v4.3.3
            )
    FetchContent_GetProperties(libzmq)
    if(NOT libzmq_POPULATED)
        message("Populating: ZeroMQ")
        FetchContent_Populate(libzmq)
        set(ZMQ_BUILD_TESTS OFF CACHE INTERNAL "Build tests for ZeroMQ" FORCE)
        add_subdirectory(${libzmq_SOURCE_DIR} ${libzmq_BINARY_DIR})
    endif()

    FetchContent_Declare(cppzmq
            GIT_REPOSITORY  https://github.com/zeromq/cppzmq.git
            GIT_TAG         v4.7.1
            )
    FetchContent_GetProperties(cppzmq)
    if(NOT cppzmq_POPULATED)
        message("Populating: cppzmq")
        FetchContent_Populate(cppzmq)
        set(CPPZMQ_BUILD_TESTS OFF CACHE INTERNAL "Build tests for cppzmq" FORCE)
        add_subdirectory(${cppzmq_SOURCE_DIR} ${cppzmq_BINARY_DIR})
    endif()

    FetchContent_Declare(gflags
            GIT_REPOSITORY  https://github.com/gflags/gflags.git
            GIT_TAG         v2.2.2
            )
    FetchContent_GetProperties(gflags)
    if(NOT gflags_POPULATED)
        message("Populating: gflags")
        FetchContent_Populate(gflags)
        set(GFLAGS_BUILD_TESTING OFF CACHE INTERNAL "Build tests")
        # Needed to work with glog
        set(GFLAGS_NAMESPACE "google" CACHE INTERNAL "Set namespace for gflags")
        add_subdirectory(${gflags_SOURCE_DIR} ${gflags_BINARY_DIR})
    endif()

    FetchContent_Declare(glog
            GIT_REPOSITORY  https://github.com/google/glog.git
            GIT_TAG         v0.4.0
            )
    FetchContent_GetProperties(glog)
    if(NOT glog_POPULATED)
        message("Populating: glog")
        FetchContent_Populate(glog)
        set(WITH_GFLAGS FALSE CACHE INTERNAL "Build glog with gflags")
        set(BUILD_TESTING FALSE CACHE INTERNAL "Build tests")
        add_subdirectory(${glog_SOURCE_DIR} ${glog_BINARY_DIR})
    endif()

    FetchContent_Declare(rapidjson
            GIT_REPOSITORY  https://github.com/Tencent/rapidjson
            GIT_TAG         v1.1.0
            GIT_SHALLOW     TRUE
            )
    FetchContent_GetProperties(rapidjson)
    if (NOT rapidjson_POPULATED)
        message("Populating: rapidjson")
        FetchContent_Populate(rapidjson)
        add_library(rapidjson INTERFACE)
        target_include_directories(rapidjson INTERFACE "${rapidjson_SOURCE_DIR}/include")
    endif()

    FetchContent_Declare(protobuf
            GIT_REPOSITORY  https://github.com/protocolbuffers/protobuf.git
            GIT_TAG         v3.20.1
            )
    FetchContent_GetProperties(protobuf)
    if (NOT protobuf_POPULATED)
        message("Populating: protobuf")
        FetchContent_Populate(protobuf)
        set(protobuf_BUILD_TESTS OFF CACHE INTERNAL "Build protobuf tests" FORCE)
        add_subdirectory(${protobuf_SOURCE_DIR}/cmake ${protobuf_BINARY_DIR})
    endif()
endif()


#========================================
#               Protobuf
#========================================

# Call this so that we can use protobuf_generate_cpp and protobuf_generate_python
# Other results are discarded
set(Protobuf_USE_STATIC_LIBS ON)
set(Protobuf_PREFIX_PATH
    "/usr/local/include"
    "/usr/local/lib"
    "/usr/local/bin"
)
list(APPEND CMAKE_PREFIX_PATH "${Protobuf_PREFIX_PATH}")
find_package(Protobuf REQUIRED)

# Generating code from protobuf files is done here instead of in a CMakeLists.txt
# in the 'proto' directory because, otherwise, we cannot do absolute import in the
# .proto files as well as in Python.

# Turn this off so that "protobuf_generate_cpp" won't append directory
# of each .proto file to the protobuf include path, which should contain only
# the project root directory. The reason is that protoc uses the longest path
# to match with the prefix of each file name, generating relative import instead
# of absolute import. For example, if the include path has:
#   "./something/proto" and "./"
# The path "./something/proto/messageA.proto" will become "messageA.proto",
# because it is matched with "./something/proto". On the other hand, if only
# "./" is present, we get "something/proto/messageA.proto", which is absolute
# with respect to the project root "./"
set(PROTOBUF_GENERATE_CPP_APPEND_PATH OFF)
set(PROTO_PREFIX proto)

protobuf_generate_cpp(PROTO_CPP_SRCS PROTO_CPP_HEADERS
        ${PROTO_PREFIX}/client.proto
        ${PROTO_PREFIX}/message.proto
        ${PROTO_PREFIX}/server.proto
        ${PROTO_PREFIX}/storage.proto
        ${PROTO_PREFIX}/transaction.proto
        ${PROTO_PREFIX}/node.proto
        )

message("${PROTO_PREFIX}/message.proto")
message ("PROTO_SRCS = ${PROTO_CPP_SRCS}")
message ("PROTO_HDRS = ${PROTO_CPP_HEADERS}")

add_library(proto ${PROTO_CPP_SRCS} ${PROTO_CPP_HEADERS} include/test/test.h)
target_include_directories(proto PUBLIC ${CMAKE_BINARY_DIR})
target_link_libraries(proto PUBLIC protobuf::libprotobuf)

# Generate python code
protobuf_generate_python(PROTO_PY_SRCS
        ${PROTO_PREFIX}/client.proto
        ${PROTO_PREFIX}/server.proto
        ${PROTO_PREFIX}/storage.proto
        ${PROTO_PREFIX}/transaction.proto
        ${PROTO_PREFIX}/message.proto
        )
# Copy generated python code to the "tools" directory
set(PROTO_PY_DEST_DIR ${CMAKE_SOURCE_DIR}/tools)
set(PROTO_PY_DEST_SRCS "")
foreach(FULL_PATH ${PROTO_PY_SRCS})
    # /base/path/relative/path/file_name.py => relative/path/file_name.py
    file(
            RELATIVE_PATH
            RELATIVE_DIR_AND_FILE_NAME
            ${CMAKE_BINARY_DIR}
            ${FULL_PATH})

    # relative/path/file_name.py => relative/path
    get_filename_component(RELATIVE_DIR ${RELATIVE_DIR_AND_FILE_NAME} DIRECTORY)

    set(PROTO_PY_DEST_SRC ${PROTO_PY_DEST_DIR}/${RELATIVE_DIR_AND_FILE_NAME})
    add_custom_command(
            OUTPUT ${PROTO_PY_DEST_SRC}
            COMMAND mkdir -p ${PROTO_PY_DEST_DIR}/${RELATIVE_DIR} && cp ${FULL_PATH} ${PROTO_PY_DEST_SRC}
            DEPENDS ${FULL_PATH}
    )
    list(APPEND PROTO_PY_DEST_SRCS ${PROTO_PY_DEST_SRC})
endforeach(FULL_PATH)
# Trigger the python code generation and copy commands
add_custom_target(proto_py ALL DEPENDS ${PROTO_PY_DEST_SRCS})

#========================================
#            Build flags
#========================================

set(CMAKE_CXX_FLAGS "-Wall -Wextra")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")
set(CMAKE_CXX_FLAGS_DEBUG  "-g -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS_ASAN "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address")
set(CMAKE_CXX_FLAGS_UBSAN "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=undefined")
set(CMAKE_CXX_FLAGS_TSAN "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=thread")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELEASE} ${CMAKE_CXX_FLAGS_DEBUG} -DNDEBUG")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

#========================================
#            Library
#========================================
include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(include)
#set(CORE_SRC_DIRS src/)
#add_library(taas_core STATIC ${SOURCE_CORE})
#foreach (DIR ${CORE_SRC_DIRS})
#    add_subdirectory(${DIR})
#endforeach()
#target_include_directories(taas_core PUBLIC ${CMAKE_SOURCE_DIR})
#target_link_libraries(taas_core
#        PUBLIC
#        proto
#        glog::glog
#        cppzmq-static
#        rapidjson)

add_subdirectory("src")

add_executable(Taas main.cpp)
# add the include directory to our compile directives
target_include_directories(Taas PUBLIC taas_core proto gflags cppzmq)

##add library path
#target_link_directories(Taas PUBLIC ${CMAKE_SOURCE_DIR})

# at the 0mq library to our link directive
target_link_libraries(Taas taas_core proto gflags cppzmq pthread dl ssl crypto)