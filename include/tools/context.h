//
// Created by 周慰星 on 11/8/22.
//

#ifndef TAAS_CONTEXT_H
#define TAAS_CONTEXT_H
#include <vector>
#include <string>

namespace Taas {
    class Context {
    public:
        explicit Context() {
            GetTaaSServerInfo("../TaaS_config.xml");
            GetStorageInfo("../Storage_config.xml");
        }
        explicit Context(const std::string& TaaS_config_file_path, const std::string& Storage_config_file_path) {
            GetTaaSServerInfo(TaaS_config_file_path);
            GetStorageInfo(Storage_config_file_path);
        }
        /// 1: TaaS server, 2: leveldb server, 3:hbase server
        uint64_t server_type = 1;

        ///TaaS server config
        std::vector<std::string> kServerIp;
        volatile uint64_t kTxnNodeNum = 1, kBackUpNum = 1;
        uint64_t kIndexNum = 1, kEpochSize_us = 10000/** us */, txn_node_ip_index = 0,
                kWorkerThreadNum = 10, kDurationTime_us = 0,
                kTestClientNum = 2, kTestKeyRange = 1000000, kTestTxnOpNum = 10,
                kCacheMaxLength = 200000, kDelayEpochNum = 0, print_mode_size = 1000;
        volatile bool is_read_repeatable = false, is_snap_isolation = false,
                is_breakdown = false, is_sync_start = false,
                is_cache_server_available = false;
        std::string glog_path_ = "/tmp";

        /// storage info
        volatile bool is_tikv_enable = true, is_leveldb_enable = true, is_hbase_enable = true;
        std::string kMasterIp, kPrivateIp, kTiKVIP, kLevevDBIP, kHbaseIP;


        void GetTaaSServerInfo(const std::string &config_file_path = "../TaaS_config.xml");
        void GetStorageInfo(const std::string &config_file_path = "../Storage_config.xml");

    };
}


#endif //TAAS_CONTEXT_H
