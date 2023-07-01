//
// Created by 周慰星 on 11/8/22.
//

#include "epoch/epoch_manager.h"
#include "epoch/worker.h"
#include "message/message.h"
#include "leveldb_server/leveldb_server.h"
#include "storage/tikv.h"
#include "storage/mot.h"
#include "test/test.h"

#include <glog/logging.h>

#include <iostream>
#include <thread>
using namespace std;

namespace Taas {
    int main() {
        Context ctx("../TaaS_config.xml", "../Storage_config.xml");

        FLAGS_log_dir = ctx.glog_path_;
        FLAGS_alsologtostderr = 1;
        google::InitGoogleLogging("Taas-sharding");
        LOG(INFO) << "System Start\n";
        std::vector<std::unique_ptr<std::thread>> threads;

        if(ctx.server_type == 1) { ///TaaS servers
            EpochManager epochManager;
            Taas::EpochManager::ctx = ctx;
            threads.push_back(std::make_unique<std::thread>(WorkerForPhysicalThreadMain, ctx));

//        threads.push_back(std::make_unique<std::thread>(WorkerForLogicalThreadMain, ctx));
            threads.push_back(std::make_unique<std::thread>(WorkerForLogicalTxnMergeCheckThreadMain, ctx));
            threads.push_back(std::make_unique<std::thread>(WorkerForLogicalAbortSetMergeCheckThreadMain));
            threads.push_back(std::make_unique<std::thread>(WorkerForLogicalCommitCheckThreadMain));
//        threads.push_back(std::make_unique<std::thread>(WorkerForLogicalRedoLogPushDownCheckThreadMain, ctx));

            threads.push_back(std::make_unique<std::thread>(WorkerForLogicalReceiveAndReplyCheckThreadMain, ctx));
            threads.push_back(std::make_unique<std::thread>(WorkerForEpochAbortSendThreadMain, ctx));
            threads.push_back(std::make_unique<std::thread>(WorkerForEpochEndFlagSendThreadMain, ctx));
            threads.push_back(std::make_unique<std::thread>(WorkerForEpochBackUpEndFlagSendThreadMain, ctx));

//        for(int i = 0; i < (int)ctx.kWorkerThreadNum; i ++) {
            for(int i = 0; i < 16; i ++) {
                threads.push_back(std::make_unique<std::thread>(WorkerFroTxnMessageThreadMain, ctx, i));///merge
            }
            for(int i = 0; i < 16; i ++) {
                threads.push_back(std::make_unique<std::thread>(WorkerFroCommitThreadMain, ctx, i));///commit
            }

            threads.push_back(std::make_unique<std::thread>(WorkerForClientListenThreadMain, ctx));///client
            threads.push_back(std::make_unique<std::thread>(WorkerForClientSendThreadMain, ctx));
            if(ctx.kTxnNodeNum > 1) {
                for(int i = 0; i < 16; i ++) {
                    threads.push_back(std::make_unique<std::thread>(WorkerFroEpochMessageThreadMain, ctx, i));///Epoch message
                }
                threads.push_back(std::make_unique<std::thread>(WorkerForServerListenThreadMain, ctx));
                threads.push_back(std::make_unique<std::thread>(WorkerForServerListenThreadMain_Epoch, ctx));
                threads.push_back(std::make_unique<std::thread>(WorkerForServerSendThreadMain, ctx));
            }
            if(ctx.is_tikv_enable) {
                TiKV::tikv_client_ptr = new tikv_client::TransactionClient({ctx.kTiKVIP});
//            for(int i = 0; i < (int)ctx.kWorkerThreadNum; i ++) {
                threads.push_back(std::make_unique<std::thread>(WorkerFroTiKVStorageThreadMain, ctx, 0));///tikv push down
//            }
            }
            threads.push_back(std::make_unique<std::thread>(WorkerFroMOTStorageThreadMain)); ///mot push down


            for(int i = 0; i < (int)ctx.kTestClientNum; i ++) {
                threads.push_back(std::make_unique<std::thread>(Client, ctx, i));
            }
        }
        else if(ctx.server_type == 2) { ///leveldb server
            ///todo : add brpc
            LevelDBServer(ctx);

        }
        else if(ctx.server_type == 3) { ///hbase server

        }



        if(ctx.kDurationTime_us != 0) {
            while(!test_start.load()) usleep(sleep_time);
            usleep(ctx.kDurationTime_us);
            EpochManager::SetTimerStop(true);
        }
        for(auto &i : threads) {
            i->join();
        }

        std::cout << "============================================================================" << std::endl;
        std::cout << "=====================              END                 =====================" << std::endl;
        std::cout << "============================================================================" << std::endl;

        return 0;
    }

}

int main() {
    Taas::main();
}