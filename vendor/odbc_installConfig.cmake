cmake_minimum_required(VERSION 3.14...3.22)

CPMAddPackage(
        NAME odbc
        VERSION 2.3.12
        URL https://www.unixodbc.org/unixODBC-2.3.12.tar.gz
        DOWNLOAD_ONLY True
)

set(ODBC_LIB "${PROJECT_BINARY_DIR}/include/sql.h")

if(NOT EXISTS "${ODBC_LIB}")
    message("Start configure odbc")
    include(ProcessorCount)
    ProcessorCount(N)
    # configure
    execute_process(COMMAND ${odbc_SOURCE_DIR}/configure --prefix=${PROJECT_BINARY_DIR} --enable-gui=no
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${odbc_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "CMake step for odbc failed: ${result}")
    endif()

#    # build and install module
    set(remake OFF BOOL)
    execute_process(COMMAND make -j ${N}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${odbc_SOURCE_DIR})
    if(result)
        execute_process(COMMAND autoreconf -ivf
                RESULT_VARIABLE result_1
                WORKING_DIRECTORY ${odbc_SOURCE_DIR})
        set(remake ON BOOL)
        if(result_1)
            message(FATAL_ERROR "Build step for odbc failed: ${result}")
        endif()
    endif()

    if(remake)
        execute_process(COMMAND make -j ${N}
                RESULT_VARIABLE result
                WORKING_DIRECTORY ${odbc_SOURCE_DIR})
        if(result)
            message(FATAL_ERROR "Build step for odbc failed: ${result}")
        endif()
    endif()

    execute_process(COMMAND make install
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${odbc_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Install step for odbc failed: ${result}")
    endif()
endif()