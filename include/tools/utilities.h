//
// Created by 周慰星 on 11/8/22.
//

#ifndef TAAS_UTILITIES_H
#define TAAS_UTILITIES_H

#pragma once

#include "proto/message.pb.h"
#include "google/protobuf/io/gzip_stream.h"
#include "google/protobuf/io/zero_copy_stream_impl.h"

#include <cstdlib>
#include <iostream>
#include <glog/logging.h>
#include <pthread.h>
#include <cstring>
#include <csignal>
#include <sched.h>
#include <thread>
#include <random>

namespace Taas {
#define UNUSED_VALUE(v) (void)(v);

    bool Gzip(google::protobuf::MessageLite* ptr, std::string* serialized_str_ptr);

    bool UnGzip(google::protobuf::MessageLite* ptr, const std::string* str);

    void SetCPU();

    void signalHandler(int signal);

    void SetScheduling(std::thread &th, int policy, int priority);

    uint64_t now_to_us();

    std::string RandomString(int length);

    uint64_t RandomNumber(uint64_t minn,uint64_t maxx);
}



#endif //TAAS_UTILITIES_H
