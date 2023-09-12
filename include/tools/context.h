//
// Created by 周慰星 on 11/8/22.
//

#ifndef TAAS_CONTEXT_H
#define TAAS_CONTEXT_H

#pragma once

#include <vector>
#include <string>
#include "blocking_mpmc_queue.h"
#include "blocking_concurrent_queue.hpp"

template<typename T>
using  BlockingConcurrentQueue = moodycamel::BlockingConcurrentQueue<T>;
//using  BlockingConcurrentQueue = BlockingMPMCQueue<T>;

template<typename T>
using  MessageBlockingConcurrentQueue = moodycamel::BlockingConcurrentQueue<T>;

namespace Taas {
    enum ServerMode {
        Taas = 1,
        LevelDB = 2,
        HBase = 3,
        MultiModelClient = 4,
    };
    enum TaasMode {
        MultiMaster = 1,
        Sharding = 2,
        TwoPC = 3,
        MultiModel = 4
    };

    class TaasContext {
    public:
        explicit TaasContext() {
            GetTaaSServerInfo("../TaaS_config.xml");
        }
        explicit TaasContext(const std::string& TaaS_config_file_path, const std::string& Storage_config_file_path) {
            GetTaaSServerInfo(TaaS_config_file_path);
        }
        /// 1: TaaS server, 2: leveldb server, 3:hbase server
        ServerMode server_type = ServerMode::Taas;

        ///TaaS server config
        TaasMode taasMode = TaasMode::MultiMaster;
        std::vector<std::string> kServerIp;
        uint64_t kTxnNodeNum = 1, kBackUpNum = 1;
        uint64_t kIndexNum = 1, kEpochSize_us = 10000/** us */, txn_node_ip_index = 0, kDurationTime_us = 0,
                kCacheMaxLength = 200000, kDelayEpochNum = 0, print_mode_size = 1000;
        uint64_t kMergeThreadNum = 10, kCommitThreadNum = 10, kEpochTxnThreadNum = 10, kEpochMessageThreadNum = 10;
        uint64_t kTestClientNum = 2, kTestKeyRange = 1000000, kTestTxnOpNum = 10;

        bool is_read_repeatable = false, is_snap_isolation = false,
                is_breakdown = false, is_sync_start = false,
                is_cache_server_available = false;
        std::string glog_path_ = "/tmp";

        /// storage info
        bool is_tikv_enable = true, is_leveldb_enable = true, is_hbase_enable = true, is_mot_enable = true;
        std::string kMasterIp, kPrivateIp, kTiKVIP, kLevelDBIP, kHbaseIP;
        uint64_t kTikvThreadNum = 10, kLeveldbThreadNum = 10, kHbaseTxnThreadNum = 10, kMOTThreadNum = 10;


        void GetTaaSServerInfo(const std::string &config_file_path = "../TaaS_config.xml");

        std::string Print();
    };

    class StorageContext {
    public:
        explicit StorageContext() {
            GetStorageInfo("../Storage_config.xml");
        }
        explicit StorageContext(const std::string& Storage_config_file_path) {
            GetStorageInfo(Storage_config_file_path);
        }

        /// storage info
        bool is_tikv_enable = true, is_leveldb_enable = true, is_hbase_enable = true, is_mot_enable = true;
        std::string kMasterIp, kPrivateIp, kTiKVIP, kLevelDBIP, kHbaseIP;
        uint64_t kTikvThreadNum = 10, kLeveldbThreadNum = 10, kHbaseTxnThreadNum = 10, kMOTThreadNum = 10;


        void GetStorageInfo(const std::string &config_file_path = "../Storage_config.xml");

    };

    enum TestMode {
        MultiModelTest = 1,
        KV = 2,
        SQL = 3,
        GQL = 4
    };

    class MultiModelContext {
    public:

        explicit MultiModelContext() {
            GetMultiModelInfo("../MultiModel_config.xml");
        }
        explicit MultiModelContext(const std::string& MultiModel_config_file_path) {
            GetMultiModelInfo(MultiModel_config_file_path);
        }

        std::string  kMultiModelClientIP, kTaasIP,
                kNebulaIP, kNebulaSpace, kNebulaUser, kNebulaPwd,
                kMOTIP, kMOTDsnName, kMOTDsnUid, kMOTDsnPwd;
        TestMode kTestMode = MultiModelTest;
        bool isLoadData = true , isUseMot = true, isUseNebula = true;

        uint64_t kRecordCount = 1000000, kTxnNum = 10000, kWriteNum = 100, kReadNum = 0,kOpNum = 10,kClientNum = 10;
        std::string kDistribution = "zipfian";

        void GetMultiModelInfo(const std::string &config_file_path = "../MultiModel_config.xml");
    };

    class Context {
    public:
        TaasContext taasContext;
        StorageContext storageContext;
        MultiModelContext multiModelContext;
    };
}

#endif //TAAS_CONTEXT_H
