//
// Created by 周慰星 on 11/8/22.
//

#include "epoch/epoch_manager.h"
#include "worker/worker_epoch_manager.h"
#include "worker/worker_epoch_merge.h"
#include "worker/worker_message.h"
#include "worker/worker_storage.h"

#include "leveldb_server/leveldb_server.h"
#include "storage/tikv.h"
#include "test/test.h"

#include "workload/multi_model_workload.h"
#include "workload/worker.h"

#include <glog/logging.h>

#include <iostream>
#include <thread>
#include <future>


using namespace std;

namespace Taas {

    int main() {
        FLAGS_log_dir = "/tmp";
        FLAGS_alsologtostderr = true;
        google::InitGoogleLogging("Taas-shard");
        LOG(INFO) << "System Start\n";
        auto res = TaasContext::Print();
        LOG(INFO) << res;
        printf("%s\n", res.c_str());
        std::vector<std::unique_ptr<std::thread>> threads;
        int cnt = 0;

        auto server_num = TaasContext::kTxnNodeNum,
                shard_num = TaasContext::kShardNum,
                replica_num = TaasContext::kReplicaNum,
                local_server_id = TaasContext::txn_node_ip_index,
                max_length = TaasContext::kCacheMaxLength;
        std::vector<std::vector<bool>> is_local_shard;
        is_local_shard.resize(server_num);
        for(auto &i : is_local_shard) {
            i.resize(shard_num);
        }
        for(uint64_t server_id = 0; server_id < server_num; server_id ++) {
            for(uint64_t i = 0; i < shard_num; i ++) {
                for(uint64_t j = 0; j < replica_num; j ++ ) {
                    if((i + server_num + j) % server_num == server_id) {
                        is_local_shard[server_id][i] = true;
                    }
                }
            }
        }
        std::string s = "";
        for(uint64_t server_id = 0; server_id < server_num; server_id ++) {
            for(uint64_t i = 0; i < shard_num; i ++) {
                if(is_local_shard[server_id][i]) {
                    s += "1";
                }
                else {
                    s += "0";
                }
            }
            s += "\n";
        }

        LOG(INFO) << "============================";
        LOG(INFO) << "shard replication statues:\n" << s;
        LOG(INFO) << "============================\n";
        if(TaasContext::server_type == ServerMode::Taas) { ///TaaS servers
            EpochManager epochManager;
            threads.push_back(std::make_unique<std::thread>(WorkerForPhysicalThreadMain)); cnt++;
            threads.push_back(std::make_unique<std::thread>(WorkerForLogicalThreadMain)); cnt++;
            threads.push_back(std::make_unique<std::thread>(WorkerForLogicalRedoLogPushDownCheckThreadMain)); cnt++;
            threads.push_back(std::make_unique<std::thread>(WorkerForEpochControlMessageThreadMain)); cnt++;

            for(int i = 0; i < (int)TaasContext::kEpochTxnThreadNum; i ++) {///handle client txn
                threads.push_back(std::make_unique<std::thread>(WorkerFroMessageThreadMain, i));  cnt++;///client txn message
            }
            for(int i = 0; i < (int)TaasContext::kEpochMessageThreadNum; i ++) {/// handle remote server message
                threads.push_back(std::make_unique<std::thread>(WorkerFroMessageEpochThreadMain, i));  cnt++;///epoch message
            }
            for(int i = 0; i < (int)TaasContext::kMergeThreadNum; i ++) {
//                threads.push_back(std::make_unique<std::thread>(WorkerFroMergeThreadMain, i));  cnt++;///merge & commit
                threads.push_back(std::make_unique<std::thread>(EpochWorkerThreadMain, i));  cnt++;
            }

            threads.push_back(std::make_unique<std::thread>(WorkerForClientListenThreadMain));  cnt++;///client
            threads.push_back(std::make_unique<std::thread>(WorkerForClientSendThreadMain)); cnt++;

            threads.push_back(std::make_unique<std::thread>(WorkerForServerListenThreadMain)); cnt++;///Server
            threads.push_back(std::make_unique<std::thread>(WorkerForServerListenThreadMain_Epoch)); cnt++;
            threads.push_back(std::make_unique<std::thread>(WorkerForServerSendThreadMain)); cnt++;
            threads.push_back(std::make_unique<std::thread>(WorkerForServerSendPUBThreadMain)); cnt++;

            ///Storage
            if(StorageContext::is_mot_enable) {
                threads.push_back(std::make_unique<std::thread>(WorkerForStorageSendMOTThreadMain)); cnt++;
                for(int i = 0; i < (int)StorageContext::kMOTThreadNum; i ++) {
                    threads.push_back(std::make_unique<std::thread>(WorkerFroMOTStorageThreadMain, i));  cnt++;///mot push down
                }
            }
            if(StorageContext::is_nebula_enable) {
                threads.push_back(std::make_unique<std::thread>(WorkerForStorageSendNebulaThreadMain)); cnt++;
                for(int i = 0; i < (int)StorageContext::kMOTThreadNum; i ++) {
                    threads.push_back(std::make_unique<std::thread>(WorkerFroNebulaStorageThreadMain, i));  cnt++;///nebula push down
                }
            }
            if(StorageContext::is_tikv_enable) {
                TiKV::tikv_client_ptr = new tikv_client::TransactionClient({StorageContext::kTiKVIP});
                for(int i = 0; i < (int)StorageContext::kTikvThreadNum; i ++) {
                    threads.push_back(std::make_unique<std::thread>(WorkerFroTiKVStorageThreadMain, i)); cnt++;///tikv push down
                }
            }
            if(StorageContext::is_leveldb_enable) {
                threads.push_back(std::make_unique<std::thread>(LevelDBServer));
                for(int i = 0; i < (int)StorageContext::kLeveldbThreadNum; i ++) {
                    threads.push_back(std::make_unique<std::thread>(WorkerFroLevelDBStorageThreadMain, i)); cnt++;///tikv push down
                }
            }
            if(StorageContext::is_hbase_enable) {
                for(int i = 0; i < (int)StorageContext::kHbaseThreadNum; i ++) {
                    threads.push_back(std::make_unique<std::thread>(WorkerFroHBaseStorageThreadMain, i)); cnt++;///tikv push down
                }
            }
//            for(int i = 0; i < 1; i ++) {
            for(int i = 0; i < (int)TaasContext::kTestClientNum; i ++) {
                if(StorageContext::is_leveldb_enable) {
                    LOG(INFO) << "LevelDBClient inserting";
                    threads.push_back(std::make_unique<std::thread>(LevelDBClient, i));
                    cnt++;
                }
                else {
                    threads.push_back(std::make_unique<std::thread>(Client, i));
                    cnt++;
                }
            }
        }
        else if(TaasContext::server_type == ServerMode::LevelDB) { ///leveldb server
            EpochManager epochManager;
            LevelDBServer();
        }
        else if(TaasContext::server_type == ServerMode::HBase) { ///hbase server
            //do nothing
        }
        else if(TaasContext::server_type == ServerMode::MultiModelClient) { ///hbase server
            workload::main();
        }



        if(TaasContext::kDurationTime_us != 0) {
            while(!test_start.load()) usleep(sleep_time);
            usleep(TaasContext::kDurationTime_us);
            EpochManager::SetTimerStop(true);
        }
//        else {
//            std::signal(SIGINT, signalHandler);
//        }
        for(auto &i : threads) {
            i->join();
        }
        google::ShutdownGoogleLogging();
        std::cout << "============================================================================" << std::endl;
        std::cout << "=====================              END                 =====================" << std::endl;
        std::cout << "============================================================================" << std::endl;
        return 0;
    }

}

int main() {

    Taas::Context ctx;
    ctx.Init();

    auto server_num = Taas::TaasContext::kTxnNodeNum,
            shard_num = Taas::TaasContext::kShardNum,
            replica_num = Taas::TaasContext::kReplicaNum,
            local_server_id = Taas::TaasContext::txn_node_ip_index,
            max_length = Taas::TaasContext::kCacheMaxLength;
    std::vector<std::vector<bool>> is_local_shard;
    is_local_shard.resize(server_num);
    for(auto &i : is_local_shard) {
        i.resize(shard_num);
    }
    for(uint64_t server_id = 0; server_id < server_num; server_id ++) {
        for(uint64_t i = 0; i < shard_num; i ++) {
            for(uint64_t j = 0; j < replica_num; j ++ ) {
                if((i + server_num + j) % server_num == server_id) {
                    is_local_shard[server_id][i] = true;
                }
            }
        }
    }
    std::string s = "";
    for(uint64_t server_id = 0; server_id < server_num; server_id ++) {
        for(uint64_t i = 0; i < shard_num; i ++) {
            if(is_local_shard[server_id][i]) {
                s += "1";
            }
            else {
                s += "0";
            }
        }
        s += "\n";
    }

    printf("============================\n");
    printf("shard replication statues:\n%s", s.c_str());
    printf("============================\n");
    Taas::main();
}