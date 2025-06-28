cmake_minimum_required(VERSION 3.14...3.22)

CPMAddPackage(
        NAME openssl
        GITHUB_REPOSITORY openssl/openssl
        VERSION v3.3.0
        GIT_TAG openssl-3.3.0
        DOWNLOAD_ONLY True
        GIT_SHALLOW TRUE
)

CPMAddPackage(
        NAME openssl_1
        VERSION v3.3.0
        URL https://github.com/openssl/openssl/releases/download/openssl-3.3.0/openssl-3.3.0.tar.gz
        DOWNLOAD_ONLY True
)



if(NOT EXISTS "${PROJECT_BINARY_DIR}/bin/openssl")
    message("Start configure openssl")
    include(ProcessorCount)
    ProcessorCount(N)
    # Call CMake to generate makefile
    execute_process(COMMAND ${openssl_SOURCE_DIR}/Configure --prefix=${PROJECT_BINARY_DIR} --libdir=${PROJECT_BINARY_DIR}/lib --openssldir=${PROJECT_BINARY_DIR}/crypto
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${openssl_SOURCE_DIR}
            OUTPUT_QUIET)
    if(result)
        message(FATAL_ERROR "Configure for openssl failed: ${result}")
    endif()

    message("Start building openssl")
    execute_process(COMMAND make -j ${N}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${openssl_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Build step for odbc failed: ${result}")
    endif()

    execute_process(COMMAND make install
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${openssl_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Install step for odbc failed: ${result}")
    endif()
#    # build and install module
#    execute_process(COMMAND make build_sw -j ${N}
#            RESULT_VARIABLE result
#            WORKING_DIRECTORY ${openssl_SOURCE_DIR}
#            OUTPUT_QUIET)
#    if(result)
#        message(FATAL_ERROR "Build step for openssl failed: ${result}")
#    endif()
#    message("Start installing openssl")
#    execute_process(COMMAND make install_sw
#            RESULT_VARIABLE result
#            WORKING_DIRECTORY ${openssl_SOURCE_DIR}
#            OUTPUT_QUIET)
#    if(result)
#        message(FATAL_ERROR "Install step for openssl failed: ${result}")
#    endif()
endif()

set(OPENSSL_ROOT_DIR ${PROJECT_BINARY_DIR})
