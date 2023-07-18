//
// Created by 周慰星 on 11/8/22.
//

#include "tools/utilities.h"

namespace Taas {


    bool Gzip(google::protobuf::MessageLite* ptr, std::string* serialized_str_ptr) {
        google::protobuf::io::GzipOutputStream::Options options;
        options.format = google::protobuf::io::GzipOutputStream::GZIP;
        options.compression_level = 9;
        google::protobuf::io::StringOutputStream outputStream(serialized_str_ptr);
        google::protobuf::io::GzipOutputStream gzipStream(&outputStream, options);
        auto res = ptr->SerializeToZeroCopyStream(&gzipStream);
        gzipStream.Close();
        return res;
    }

    bool UnGzip(google::protobuf::MessageLite* ptr, const std::string* str) {
//    auto message_string_ptr = std::make_unique<std::string>(static_cast<const char*>(message_ptr->data()), message_ptr->size());
        google::protobuf::io::ArrayInputStream inputStream(str->data(), (int)str->size());
        google::protobuf::io::GzipInputStream gzipStream(&inputStream);
        return ptr->ParseFromZeroCopyStream(&gzipStream);
    }

    std::atomic<int> cpu_index(1);
    void SetCPU(){
        #ifdef _WIN32
        #elif __linux__
            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(cpu_index.fetch_add(1), &cpuset);
            int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
//            int rc = sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);
            if (rc != 0) {
                std::cout << "Set CPU Error!!!" << std::endl;
                assert(false);
            }
        #elif __APPLE__
        #endif
    }


    /**
 * @brief 获取1970年1月1日到现在的时间间隔（以微妙为单位），随时间逐渐增大
 *
 * @return uint64_t 时间间隔差
 */
    uint64_t now_to_us(){
        // 获取系统当前时间
        auto today = std::chrono::system_clock::now();
        // 获得1970年1月1日到现在的时间间隔
        auto time_duration = today.time_since_epoch();
        // 时间间隔以微妙的形式展现
        std::chrono::microseconds ms_duration = std::chrono::duration_cast<std::chrono::microseconds>(time_duration);
        uint64_t timestamp = ms_duration.count();
        return timestamp;
    }

}