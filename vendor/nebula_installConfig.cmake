cmake_minimum_required(VERSION 3.14...3.22)

CPMAddPackage(
        NAME nebula
        GITHUB_REPOSITORY vesoft-inc/nebula-cpp
        GIT_TAG v3.4.0
        VERSION v3.4.0
        DOWNLOAD_ONLY True
        GIT_SHALLOW TRUE
)

set(NEBULA_LIB "${PROJECT_BINARY_DIR}/lib/libnebula_graph_client.so")
#set(NEBULA_LIB "${PROJECT_BINARY_DIR}/include/nebula/Session.h")

if(NOT EXISTS "${NEBULA_LIB}")
    message("Start configure nebula")

    include(ProcessorCount)
    ProcessorCount(N)
    # Call CMake to generate makefile
    execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=ON
                -DCMAKE_PREFIX_PATH=${PROJECT_BINARY_DIR} -DCMAKE_INSTALL_PREFIX=${PROJECT_BINARY_DIR}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${nebula_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "CMake step for nebula failed: ${result}")
    endif()
    # build and install module
#    execute_process(COMMAND make -j ${N}
    execute_process(COMMAND ${CMAKE_COMMAND} --build build --config Release -j ${N}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${nebula_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Build step for nebula failed: ${result}")
    endif()

    execute_process(COMMAND ${CMAKE_COMMAND} --install build
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${nebula_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Install step for nebula failed: ${result}")
    endif()

endif()
