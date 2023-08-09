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

#include <glog/logging.h>

#include <iostream>
#include <thread>
#include <pthread.h>
#include <cstring>
#include <csignal>
#include <sched.h>

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

    void signalHandler(int signal) {
        if (signal == SIGINT)
        {
            std::cout << "Ctrl+C detected!" << std::endl;
            Taas::EpochManager::SetTimerStop(true);
        }
    }

    static sched_param sch_params;
    void SetScheduling(std::thread &th, int policy, int priority) {
        sch_params.sched_priority = priority;
        if (pthread_setschedparam(th.native_handle(), policy, &sch_params)) {
            std::cerr << "Failed to set Thread scheduling :" << std::strerror(errno) << std::endl;
        }
    }

    int main() {
        Context ctx("../TaaS_config.xml", "../Storage_config.xml");

        FLAGS_log_dir = "/tmp";
        FLAGS_alsologtostderr = true;
        google::InitGoogleLogging("Taas-sharding");
        LOG(INFO) << "System Start\n";
        auto res = ctx.Print();
        LOG(INFO) << res;
        printf("%s\n", res.c_str());
        std::vector<std::unique_ptr<std::thread>> threads;
        int cnt = 0;
        if(ctx.server_type == ServerMode::Taas) { ///TaaS servers
            EpochManager epochManager;
            Taas::EpochManager::ctx = ctx;
            threads.push_back(std::make_unique<std::thread>(WorkerForPhysicalThreadMain, ctx));
            SetScheduling(*threads[cnt], SCHED_RR, 5); cnt ++;
            threads.push_back(std::make_unique<std::thread>(WorkerForLogicalThreadMain, ctx));
            SetScheduling(*threads[cnt], SCHED_RR, 5); cnt ++;
//            threads.push_back(std::make_unique<std::thread>(WorkerForLogicalTxnMergeCheckThreadMain, ctx));
//            threads.push_back(std::make_unique<std::thread>(WorkerForLogicalAbortSetMergeCheckThreadMain, ctx));
//            threads.push_back(std::make_unique<std::thread>(WorkerForLogicalCommitCheckThreadMain, ctx));

            threads.push_back(std::make_unique<std::thread>(WorkerForLogicalRedoLogPushDownCheckThreadMain, ctx));
            SetScheduling(*threads[cnt], SCHED_RR, 4); cnt ++;
//            threads.push_back(std::make_unique<std::thread>(WorkerForEpochControlMessageThreadMain, ctx));

            threads.push_back(std::make_unique<std::thread>(WorkerForLogicalReceiveAndReplyCheckThreadMain, ctx));
            SetScheduling(*threads[cnt], SCHED_RR, 4); cnt ++;
            threads.push_back(std::make_unique<std::thread>(WorkerForEpochAbortSendThreadMain, ctx));
            SetScheduling(*threads[cnt], SCHED_RR, 4); cnt ++;
            threads.push_back(std::make_unique<std::thread>(WorkerForEpochEndFlagSendThreadMain, ctx));
            SetScheduling(*threads[cnt], SCHED_RR, 4); cnt ++;
            threads.push_back(std::make_unique<std::thread>(WorkerForEpochBackUpEndFlagSendThreadMain, ctx));
            SetScheduling(*threads[cnt], SCHED_RR, 4); cnt ++;

            for(int i = 0; i < (int)ctx.kWorkerThreadNum; i ++) {
                threads.push_back(std::make_unique<std::thread>(WorkerFroMessageThreadMain, ctx, i));///txn message
                SetScheduling(*threads[cnt], SCHED_RR, 3); cnt ++;
                threads.push_back(std::make_unique<std::thread>(WorkerFroMessageEpochThreadMain, ctx, i));///epoch message
                SetScheduling(*threads[cnt], SCHED_RR, 4); cnt ++;
                threads.push_back(std::make_unique<std::thread>(WorkerFroMergeThreadMain, ctx, i));///merge
                SetScheduling(*threads[cnt], SCHED_RR, 3); cnt ++;
                threads.push_back(std::make_unique<std::thread>(WorkerFroCommitThreadMain, ctx, i));///commit
                SetScheduling(*threads[cnt], SCHED_RR, 3); cnt ++;
            }

            threads.push_back(std::make_unique<std::thread>(WorkerForClientListenThreadMain, ctx));///client
            SetScheduling(*threads[cnt], SCHED_RR, 4); cnt ++;
            threads.push_back(std::make_unique<std::thread>(WorkerForClientSendThreadMain, ctx));
            SetScheduling(*threads[cnt], SCHED_RR, 4); cnt ++;
            if(ctx.kTxnNodeNum > 1) {
                threads.push_back(std::make_unique<std::thread>(WorkerForServerListenThreadMain, ctx));
                SetScheduling(*threads[cnt], SCHED_RR, 4); cnt ++;
                threads.push_back(std::make_unique<std::thread>(WorkerForServerListenThreadMain_Epoch, ctx));
                SetScheduling(*threads[cnt], SCHED_RR, 4); cnt ++;
                threads.push_back(std::make_unique<std::thread>(WorkerForServerSendThreadMain, ctx));
                SetScheduling(*threads[cnt], SCHED_RR, 4); cnt ++;
            }

            ///Storage
            threads.push_back(std::make_unique<std::thread>(WorkerForStorageSendThreadMain, ctx));
            SetScheduling(*threads[cnt], SCHED_RR, 3); cnt ++;
            if(ctx.is_mot_enable) {
                for(int i = 0; i < (int)ctx.kWorkerThreadNum; i ++) {
                    threads.push_back(std::make_unique<std::thread>(WorkerFroMOTStorageThreadMain, ctx, i)); ///mot push down
                    SetScheduling(*threads[cnt], SCHED_RR, 3); cnt++;
                }
            }
            if(ctx.is_tikv_enable) {
                TiKV::tikv_client_ptr = new tikv_client::TransactionClient({ctx.kTiKVIP});
                for(int i = 0; i < (int)ctx.kWorkerThreadNum; i ++) {
                    threads.push_back(std::make_unique<std::thread>(WorkerFroTiKVStorageThreadMain, ctx, i));///tikv push down
                    SetScheduling(*threads[cnt], SCHED_RR, 3); cnt++;
                }
            }
            for(int i = 0; i < (int)ctx.kTestClientNum; i ++) {
                threads.push_back(std::make_unique<std::thread>(Client, ctx, i));
                SetScheduling(*threads[cnt], SCHED_RR, 3); cnt++;
            }
        }
        else if(ctx.server_type == ServerMode::LevelDB) { ///leveldb server
            ///todo : add brpc
            LevelDBServer(ctx);
        }
        else if(ctx.server_type == ServerMode::HBase) { ///hbase server

        }

        if(ctx.kDurationTime_us != 0) {
            while(!test_start.load()) usleep(sleep_time);
            usleep(ctx.kDurationTime_us);
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