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
        Shard = 2,
        TwoPC = 3,
        MultiModel = 4
    };

    class TaasContext {
    public:
        explicit TaasContext() {
//            GetTaaSServerInfo("../TaaS_config.xml");
        }
//        explicit TaasContext(const std::string& TaaS_config_file_path, const std::string& Storage_config_file_path) {
//            GetTaaSServerInfo(TaaS_config_file_path);
//        }
        /// 1: TaaS server, 2: leveldb server, 3:hbase server
        static ServerMode server_type;

        ///TaaS server config
        static TaasMode taasMode;
        static std::vector<std::string> kServerIp;
        static uint64_t kTxnNodeNum, kBackUpNum ;
        static uint64_t kIndexNum, kEpochSize_us, txn_node_ip_index,
                kShardNum, kReplicaNum,
                kDurationTime_us,
                kCacheMaxLength, kDelayEpochNum, print_mode_size;
        static uint64_t kMergeThreadNum, kEpochTxnThreadNum, kEpochMessageThreadNum;
        static uint64_t kTestClientNum, kTestKeyRange, kTestTxnOpNum;
        static uint64_t kHandleEpochMessageNumOfEachTraversal, kHandleTxnMessageNumOfEachTraversal, kSafeEpochDistance;

        static bool is_read_repeatable, is_snap_isolation,
                is_breakdown, is_sync_start,
                is_cache_server_available;
        static std::string glog_path;

        static void GetTaaSServerInfo(const std::string &config_file_path = "../TaaS_config.xml");

        static std::string Print();
    };

    class StorageContext {
    public:
        explicit StorageContext() {
//            GetStorageInfo("../Storage_config.xml");
        }
//        explicit StorageContext(const std::string& Storage_config_file_path) {
//            GetStorageInfo(Storage_config_file_path);
//        }

        /// storage info
//        static bool is_tikv_enable = false, is_leveldb_enable = false, is_hbase_enable = false, is_mot_enable = true, is_nebula_enable = false;
//        static std::string kMasterIp, kPrivateIp, kTiKVIP, kLevelDBIP, kHbaseIP;
//        static uint64_t kTikvThreadNum = 10, kLeveldbThreadNum = 10, kHbaseThreadNum = 10, kMOTThreadNum = 10;

        static bool is_tikv_enable, is_leveldb_enable, is_hbase_enable, is_mot_enable, is_nebula_enable;
        static std::string kMasterIp, kPrivateIp, kTiKVIP, kLevelDBIP, kHbaseIP;
        static uint64_t kTikvThreadNum, kLeveldbThreadNum, kHbaseThreadNum, kMOTThreadNum;

        static void GetStorageInfo(const std::string &config_file_path = "../Storage_config.xml");

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
//            GetMultiModelInfo("../MultiModel_config.xml");
        }
//        explicit MultiModelContext(const std::string& MultiModel_config_file_path) {
//            GetMultiModelInfo(MultiModel_config_file_path);
//        }

        static std::string  kMultiModelClientIP, kTaasIP,
                kNebulaIP, kNebulaSpace, kNebulaUser, kNebulaPwd,
                kMOTIP, kMOTDsnName, kMOTDsnUid, kMOTDsnPwd;
        static TestMode kTestMode;
        static bool isLoadData, isUseMot, isUseNebula;

        static uint64_t kRecordCount, kTxnNum, kWriteNum, kReadNum, kOpNum, kClientNum;
        static std::string kDistribution;

        void GetMultiModelInfo(const std::string &config_file_path = "../MultiModel_config.xml");
    };

    class Context {
    public:
        TaasContext taasContext;
        StorageContext storageContext;
        MultiModelContext multiModelContext;

        void Init() {
          taasContext.GetTaaSServerInfo("../TaaS_config.xml");
          storageContext.GetStorageInfo("../Storage_config.xml");
          multiModelContext.GetMultiModelInfo("../MultiModelConfig.xml");
        }
    };
}

#endif //TAAS_CONTEXT_H
