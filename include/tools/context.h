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
        std::vector<std::string> kServerIp, kCacheServerIp;
        std::vector<uint64_t> port; // ServerNum * PackageNum
        // server_num，txn node的个数
        volatile uint64_t kTxnNodeNum = 1, kBackUpNum = 1;
        uint64_t kIndexNum = 1, kEpochSize_us = 10000/** us */, txn_node_ip_index = 0, kWorkerThreadNum = 10, kSendClientThreadNum = 2,
                kMessageCacheThreadNum = 2, kPackThreadNum = 2, kTiKVSendThreadNum = 2, kDurationTime_us = 0,
                kTestClientNum = 2, kTestKeyRange = 1000000, kTestTxnOpNum = 10,
                kCacheMaxLength = 200000, kDelayEpochNum = 0, kServerTimeOut_us = 700000,
                kRaftTimeOut_us = 500000, kStartCheckStateNum = 1000000;
        std::vector<std::string> send_ips;
        std::vector<uint64_t>send_ports;
        std::string kMasterIp, kPrivateIp, kTiKVIP;
        volatile bool is_read_repeatable = false, is_breakdown = false, is_sync_start = false,
                is_snap_isolation = false, is_cache_server_available = false, is_fault_tolerance_enable = false, is_sync_exec = false,
                is_full_async_exec = false, is_total_pack = false, is_protobuf_gzip = true, is_tikv_enable = true;


        void GetServerInfo();
    };
}


#endif //TAAS_CONTEXT_H
