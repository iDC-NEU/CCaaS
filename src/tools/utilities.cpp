//
// Created by 周慰星 on 11/8/22.
//

#include "tools/utilities.h"
#include "_deps/protobuf-src/src/google/protobuf/io/gzip_stream.h"
#include "_deps/protobuf-src/src/google/protobuf/io/zero_copy_stream_impl.h"

namespace Taas {


    bool Gzip(google::protobuf::MessageLite* ptr, std::string* serialized_str_ptr) {
        google::protobuf::io::GzipOutputStream::Options options;
        options.format = google::protobuf::io::GzipOutputStream::GZIP;
        options.compression_level = 9;
        google::protobuf::io::StringOutputStream outputStream(serialized_str_ptr);
        google::protobuf::io::GzipOutputStream gzipStream(&outputStream, options);
        ptr->SerializeToZeroCopyStream(&gzipStream);
        gzipStream.Close();
        return true;
    }

    bool UnGzip(google::protobuf::MessageLite* ptr, const std::string* str) {
//    auto message_string_ptr = std::make_unique<std::string>(static_cast<const char*>(message_ptr->data()), message_ptr->size());
        google::protobuf::io::ArrayInputStream inputStream(str->data(), (int)str->size());
        google::protobuf::io::GzipInputStream gzipStream(&inputStream);
        ptr->ParseFromZeroCopyStream(&gzipStream);
        return true;
    }

    std::atomic<int> cpu_index(1);

    void SetCPU(){
//        #ifdef _WIN32
//        #elif __linux__
//            cpu_set_t logicalEpochSet;
//            CPU_ZERO(&logicalEpochSet);
//            CPU_SET(cpu_index.fetch_add(1), &logicalEpochSet); //2就是核心号
//            int rc = sched_setaffinity(0, sizeof(cpu_set_t), &logicalEpochSet);
//            if (rc == -1) {
//                assert(false);
//            }
//        #elif __APPLE__
//        #endif
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