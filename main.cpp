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
#include <csignal>

using namespace std;

namespace Taas {
    void signalHandler(int signal)
    {
        if (signal == SIGINT)
        {
            std::cout << "Ctrl+C detected!" << std::endl;
            EpochManager::SetTimerStop(true);
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

        if(ctx.server_type == ServerMode::Taas) { ///TaaS servers
            EpochManager epochManager;
            Taas::EpochManager::ctx = ctx;
            threads.push_back(std::make_unique<std::thread>(WorkerForPhysicalThreadMain, ctx));
            threads.push_back(std::make_unique<std::thread>(WorkerForLogicalThreadMain, ctx));

//            threads.push_back(std::make_unique<std::thread>(WorkerForLogicalTxnMergeCheckThreadMain, ctx));
//            threads.push_back(std::make_unique<std::thread>(WorkerForLogicalAbortSetMergeCheckThreadMain, ctx));
//            threads.push_back(std::make_unique<std::thread>(WorkerForLogicalCommitCheckThreadMain, ctx));

            threads.push_back(std::make_unique<std::thread>(WorkerForLogicalRedoLogPushDownCheckThreadMain, ctx));

//            threads.push_back(std::make_unique<std::thread>(WorkerForEpochControlMessageThreadMain, ctx));

            threads.push_back(std::make_unique<std::thread>(WorkerForLogicalReceiveAndReplyCheckThreadMain, ctx));
            threads.push_back(std::make_unique<std::thread>(WorkerForEpochAbortSendThreadMain, ctx));
            threads.push_back(std::make_unique<std::thread>(WorkerForEpochEndFlagSendThreadMain, ctx));
            threads.push_back(std::make_unique<std::thread>(WorkerForEpochBackUpEndFlagSendThreadMain, ctx));

            for(int i = 0; i < (int)ctx.kWorkerThreadNum; i ++) {
                threads.push_back(std::make_unique<std::thread>(WorkerFroMessageThreadMain, ctx, i));///txn message
                threads.push_back(std::make_unique<std::thread>(WorkerFroMessageEpochThreadMain, ctx, i));///epoch message
                threads.push_back(std::make_unique<std::thread>(WorkerFroMergeThreadMain, ctx, i));///merge
                threads.push_back(std::make_unique<std::thread>(WorkerFroCommitThreadMain, ctx, i));///commit
            }

            threads.push_back(std::make_unique<std::thread>(WorkerForClientListenThreadMain, ctx));///client
            threads.push_back(std::make_unique<std::thread>(WorkerForClientSendThreadMain, ctx));
            if(ctx.kTxnNodeNum > 1) {
                threads.push_back(std::make_unique<std::thread>(WorkerForServerListenThreadMain, ctx));
                threads.push_back(std::make_unique<std::thread>(WorkerForServerListenThreadMain_Epoch, ctx));
                threads.push_back(std::make_unique<std::thread>(WorkerForServerSendThreadMain, ctx));
            }

            ///Storage
            threads.push_back(std::make_unique<std::thread>(WorkerForStorageSendThreadMain, ctx));
            if(ctx.is_mot_enable) {
                for(int i = 0; i < (int)ctx.kWorkerThreadNum; i ++)
                    threads.push_back(std::make_unique<std::thread>(WorkerFroMOTStorageThreadMain, ctx, i)); ///mot push down
            }
            if(ctx.is_tikv_enable) {
                TiKV::tikv_client_ptr = new tikv_client::TransactionClient({ctx.kTiKVIP});
                for(int i = 0; i < (int)ctx.kWorkerThreadNum; i ++) {
                    threads.push_back(std::make_unique<std::thread>(WorkerFroTiKVStorageThreadMain, ctx, i));///tikv push down
                }
            }
            for(int i = 0; i < (int)ctx.kTestClientNum; i ++) {
                threads.push_back(std::make_unique<std::thread>(Client, ctx, i));
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