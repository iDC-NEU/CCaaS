cmake_minimum_required(VERSION 3.14...3.22)

CPMAddPackage(
        NAME thrift
        GITHUB_REPOSITORY apache/thrift
        VERSION 0.18.1
        GIT_TAG v0.18.1
        DOWNLOAD_ONLY True
        GIT_SHALLOW TRUE
)

if(NOT EXISTS "${PROJECT_BINARY_DIR}/include/thrift/Thrift.h")
    message("Start configure thrift")
    include(ProcessorCount)
    ProcessorCount(N)

    # Call CMake to generate makefile
    execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" -B build -DBUILD_CPP=true -DBUILD_C_GLIB=false -DBUILD_JAVA=false -DBUILD_JAVASCRIPT=false -DBUILD_NODEJS=false -DBUILD_KOTLIN=false -DBUILD_PYTHON=false -DCMAKE_PREFIX_PATH=${PROJECT_BINARY_DIR} -DCMAKE_INSTALL_PREFIX=${PROJECT_BINARY_DIR}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${thrift_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "CMake step for thrift failed: ${result}")
    endif()

    # build and install module
    execute_process(COMMAND ${CMAKE_COMMAND} --build build --config Release -- -j ${N}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${thrift_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Build step for thrift failed: ${result}")
    endif()

    execute_process(COMMAND ${CMAKE_COMMAND} --install build
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${thrift_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Install step for thrift failed: ${result}")
    endif()
endif()
