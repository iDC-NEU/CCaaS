//
// Created by 周慰星 on 11/8/22.
//

#include "tools/utilities.h"
#include "epoch/epoch_manager.h"

namespace Taas {


    bool Gzip(google::protobuf::MessageLite* ptr, std::string* serialized_str_ptr) {
//        google::protobuf::io::GzipOutputStream::Options options;
//        options.format = google::protobuf::io::GzipOutputStream::GZIP;
//        options.compression_level = 9;
//        google::protobuf::io::StringOutputStream outputStream(serialized_str_ptr);
//        google::protobuf::io::GzipOutputStream gzipStream(&outputStream, options);
//        auto res = ptr->SerializeToZeroCopyStream(&gzipStream);
//        gzipStream.Close();

        google::protobuf::io::StringOutputStream outputStream(serialized_str_ptr);
        auto res = ptr->SerializeToZeroCopyStream(&outputStream);
        return res;
    }

    bool UnGzip(google::protobuf::MessageLite* ptr, const std::string* str) {
//    auto message_string_ptr = std::make_unique<std::string>(static_cast<const char*>(message_ptr->data()), message_ptr->size());
//        google::protobuf::io::ArrayInputStream inputStream(str->data(), (int)str->size());
//        google::protobuf::io::GzipInputStream gzipStream(&inputStream);
//        return ptr->ParseFromZeroCopyStream(&gzipStream);
        google::protobuf::io::ArrayInputStream inputStream(str->data(), (int)str->size());
        return ptr->ParseFromZeroCopyStream(&inputStream);
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
//                assert(false);
            }
        #elif __APPLE__
        #endif
    }

    void signalHandler(int signal) {
        if (signal == SIGINT){
            std::cout << "Ctrl+C detected!" << std::endl;
            EpochManager::SetTimerStop(true);
        }
    }

    static sched_param sch_params;
    void SetScheduling(std::thread &th, int policy, int priority) {
        sch_params.sched_priority = priority;
        if (pthread_setschedparam(th.native_handle(), policy, &sch_params)) {
            std::cerr << "Failed to set Thread scheduling :" << std::strerror(errno) << std::endl;
        }
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

    std::string RandomString(int length) {			// length: 产生字符串的长度
        char tmp;							// tmp: 暂存一个随机数
        std::string buffer;						// buffer: 保存返回值
        // 下面这两行比较重要:
        std::random_device rd;					// 产生一个 std::random_device 对象 rd
        std::default_random_engine random(rd());	// 用 rd 初始化一个随机数发生器 random
        for (int i = 0; i < length; i++) {
            tmp = random() % 36;	// 随机一个小于 36 的整数，0-9、A-Z 共 36 种字符
            if (tmp < 10) {			// 如果随机数小于 10，变换成一个阿拉伯数字的 ASCII
                tmp += '0';
            } else {				// 否则，变换成一个大写字母的 ASCII
                tmp -= 10;
                tmp += 'A';
            }
            buffer += tmp;
        }
        return buffer;
    }

    uint64_t RandomNumber(uint64_t minn,uint64_t maxx){
        uint64_t res;
        std::random_device rd;
        std::default_random_engine random(rd());
        res = random()%(maxx-minn)+minn;
        return res;
    }

}