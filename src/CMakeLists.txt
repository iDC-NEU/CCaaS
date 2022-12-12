#add_subdirectory(epoch message transaction utils)
#aux_source_directory(. DIR_SRCS)
file(GLOB SOURCE
        connection/*.cpp
        epoch/*.cpp
        message/*.cpp
        test/*.cpp
        tools/*.cpp
        transaction/*.cpp
        utils/*.cpp
        *.cpp)

add_library(taas_core ${SOURCE})
add_library(Taas::core ALIAS taas_core)

target_include_directories(taas_core PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(taas_core
        PUBLIC
        proto
        glog::glog
        cppzmq-static
        rapidjson)

#
#target_sources(taas_core
#        PRIVATE
#        ${DIR_SRCS})