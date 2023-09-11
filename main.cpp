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

//class Thread {
//public:
//    Thread() = default;
//    static void setScheduling(std::thread &th, int policy, int priority) {
//        sch_params.sched_priority = priority;
//        if(pthread_setschedparam(th.native_handle(), policy, &sch_params)) {
//            std::cerr <<"Failed to set Thread scheduling :" << std::strerror(errno) << std::endl;
//        }
//    }
//private:
//    static sched_param sch_params;
//};

namespace Taas {

    int main() {
        Context ctx;

        FLAGS_log_dir = "/tmp";
        FLAGS_alsologtostderr = true;
        google::InitGoogleLogging("Taas-sharding");
        LOG(INFO) << "System Start\n";
        auto res = ctx.taasContext.Print();
        LOG(INFO) << res;
        printf("%s\n", res.c_str());
        std::vector<std::unique_ptr<std::thread>> threads;
        int cnt = 0;
        if(ctx.taasContext.server_type == ServerMode::Taas) { ///TaaS servers
            EpochManager epochManager;
            Taas::EpochManager::ctx = ctx;
            threads.push_back(std::make_unique<std::thread>(WorkerForPhysicalThreadMain, ctx)); cnt++;
            threads.push_back(std::make_unique<std::thread>(WorkerForLogicalThreadMain, ctx)); cnt++;
            threads.push_back(std::make_unique<std::thread>(WorkerForLogicalRedoLogPushDownCheckThreadMain, ctx)); cnt++;
//            threads.push_back(std::make_unique<std::thread>(WorkerForLogicalTxnMergeCheckThreadMain, ctx)); cnt++;
//            threads.push_back(std::make_unique<std::thread>(WorkerForLogicalAbortSetMergeCheckThreadMain, ctx)); cnt++;
//            threads.push_back(std::make_unique<std::thread>(WorkerForLogicalCommitCheckThreadMain, ctx)); cnt++;

//            threads.push_back(std::make_unique<std::thread>(WorkerForEpochControlMessageThreadMain, ctx)); cnt++;

//            threads.push_back(std::make_unique<std::thread>(WorkerForLogicalReceiveAndReplyCheckThreadMain, ctx)); cnt++;
//            threads.push_back(std::make_unique<std::thread>(WorkerForEpochAbortSendThreadMain, ctx)); cnt++;
//            threads.push_back(std::make_unique<std::thread>(WorkerForEpochEndFlagSendThreadMain, ctx)); cnt++;
//            threads.push_back(std::make_unique<std::thread>(WorkerForEpochBackUpEndFlagSendThreadMain, ctx)); cnt++;

            for(int i = 0; i < (int)ctx.taasContext.kEpochTxnThreadNum; i ++) {///handle client txn
                threads.push_back(std::make_unique<std::thread>(WorkerFroMessageThreadMain, ctx, i));  cnt++;///txn message
            }
            for(int i = 0; i < (int)ctx.taasContext.kEpochMessageThreadNum; i ++) {/// handle remote server message
                threads.push_back(std::make_unique<std::thread>(WorkerFroMessageEpochThreadMain, ctx, i));  cnt++;///epoch message
            }
            for(int i = 0; i < (int)ctx.taasContext.kMergeThreadNum; i ++) {
                threads.push_back(std::make_unique<std::thread>(WorkerFroMergeThreadMain, ctx, i));  cnt++;///merge & commit
            }

            threads.push_back(std::make_unique<std::thread>(WorkerForClientListenThreadMain, ctx));  cnt++;///client
            threads.push_back(std::make_unique<std::thread>(WorkerForClientSendThreadMain, ctx)); cnt++;
            if(ctx.taasContext.kTxnNodeNum > 1) {
                threads.push_back(std::make_unique<std::thread>(WorkerForServerListenThreadMain, ctx)); cnt++;
                threads.push_back(std::make_unique<std::thread>(WorkerForServerListenThreadMain_Epoch, ctx)); cnt++;
                threads.push_back(std::make_unique<std::thread>(WorkerForServerSendThreadMain, ctx)); cnt++;
                threads.push_back(std::make_unique<std::thread>(WorkerForServerSendPUBThreadMain, ctx)); cnt++;
            }

            ///Storage
            threads.push_back(std::make_unique<std::thread>(WorkerForStorageSendThreadMain, ctx)); cnt++;
//            kTikvThreadNum = 10, kLeveldbThreadNum = 10, kHbaseTxnThreadNum = 10, kMOTThreadNum = 10;
            if(ctx.storageContext.is_mot_enable) {
                for(int i = 0; i < (int)ctx.storageContext.kMOTThreadNum; i ++) {
                    threads.push_back(std::make_unique<std::thread>(WorkerFroMOTStorageThreadMain, ctx, i));  cnt++;///mot push down
                }
            }
            if(ctx.storageContext.is_tikv_enable) {
                TiKV::tikv_client_ptr = new tikv_client::TransactionClient({ctx.storageContext.kTiKVIP});
                for(int i = 0; i < (int)ctx.storageContext.kTikvThreadNum; i ++) {
                    threads.push_back(std::make_unique<std::thread>(WorkerFroTiKVStorageThreadMain, ctx, i)); cnt++;///tikv push down
                }
            }
            if(ctx.storageContext.is_leveldb_enable) {
                for(int i = 0; i < (int)ctx.storageContext.kLeveldbThreadNum; i ++) {
                    threads.push_back(std::make_unique<std::thread>(WorkerFroLevelDBStorageThreadMain, ctx, i)); cnt++;///tikv push down
                }
            }
            if(ctx.storageContext.is_hbase_enable) {
                for(int i = 0; i < (int)ctx.storageContext.kHbaseTxnThreadNum; i ++) {
                    threads.push_back(std::make_unique<std::thread>(WorkerFroHBaseStorageThreadMain, ctx, i)); cnt++;///tikv push down
                }
            }

            for(int i = 0; i < (int)ctx.taasContext.kTestClientNum; i ++) {
                if(ctx.storageContext.is_leveldb_enable) {
                    threads.push_back(std::make_unique<std::thread>(LevelDBClient, ctx, i));
                    cnt++;
                }
                else {
                    threads.push_back(std::make_unique<std::thread>(Client, ctx, i));
                    cnt++;
                }
            }
        }
        else if(ctx.taasContext.server_type == ServerMode::LevelDB) { ///leveldb server
            EpochManager epochManager;
            Taas::EpochManager::ctx = ctx;
            LevelDBServer(ctx);
        }
        else if(ctx.taasContext.server_type == ServerMode::HBase) { ///hbase server
            //do nothing
        }
        else if(ctx.taasContext.server_type == ServerMode::MultiModelServer) { ///hbase server
            ///todo: multimodel server
        }
        else if(ctx.taasContext.server_type == ServerMode::MultiModelClient) { ///hbase server
            workload::main();
        }



        if(ctx.taasContext.kDurationTime_us != 0) {
            while(!test_start.load()) usleep(sleep_time);
            usleep(ctx.taasContext.kDurationTime_us);
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
    Taas::main();
}