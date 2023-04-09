//
// Created by 周慰星 on 11/8/22.
//

#include <iostream>
#include <thread>
#include "epoch/epoch_manager.h"
#include "epoch/worker.h"
#include "message/message.h"
#include "storage/tikv.h"
#include "storage/mot.h"
#include "test/test.h"
using namespace std;

namespace Taas {
    int main() {
        Context ctx;
        ctx.GetServerInfo();
        EpochManager epochManager;
        Taas::EpochManager::ctx = ctx;
        printf("System Start\n");
        std::vector<std::unique_ptr<std::thread>> threads;

        threads.push_back(std::make_unique<std::thread>(EpochPhysicalTimerManagerThreadMain, ctx));
        threads.push_back(std::make_unique<std::thread>(EpochLogicalTimerManagerThreadMain, ctx));
        threads.push_back(std::make_unique<std::thread>(StateChecker, ctx));
        for(int i = 0; i < (int)ctx.kWorkerThreadNum; i ++) {
            threads.push_back(std::make_unique<std::thread>(WorkerFroMessageThreadMain, ctx, i));///merge
        }
        for(int i = 0; i < (int)ctx.kWorkerThreadNum; i ++) {
            threads.push_back(std::make_unique<std::thread>(WorkerFroCommitThreadMain, ctx, i));///commit
        }

        threads.push_back(std::make_unique<std::thread>(SendClientThreadMain, ctx));///client
        threads.push_back(std::make_unique<std::thread>(ListenClientThreadMain, ctx));

        if(ctx.is_tikv_enable) {
            for(int i = 0; i < (int)ctx.kWorkerThreadNum; i ++) {
                threads.push_back(std::make_unique<std::thread>(WorkerFroTiKVStorageThreadMain, i));///tikv push down
            }
        }
        threads.push_back(std::make_unique<std::thread>(WorkerFroMOTStorageThreadMain, ctx)); ///mot push down

        if(ctx.kTxnNodeNum > 1) {
            threads.push_back(std::make_unique<std::thread>(SendServerThreadMain, ctx));
            threads.push_back(std::make_unique<std::thread>(ListenServerThreadMain, ctx));
        }


        for(int i = 0; i < (int)ctx.kTestClientNum; i ++) {
            threads.push_back(std::make_unique<std::thread>(Client, ctx, i));
        }

        if(ctx.is_tikv_enable) {
            TiKV::tikv_client_ptr = new tikv_client::TransactionClient({ctx.kTiKVIP});
        }


        if(ctx.kDurationTime_us != 0) {
            while(!test_start.load()) usleep(10000);
            usleep(ctx.kDurationTime_us);
            EpochManager::SetTimerStop(true);
            MessageQueue::send_to_client_queue->enqueue(nullptr);
            MessageQueue::send_to_server_queue->enqueue(nullptr);

            zmq::context_t context(1);
            zmq::socket_t socket_send(context, ZMQ_PUSH);
            zmq::send_flags sendFlags = zmq::send_flags::none;
            socket_send.bind("tcp://localhost:5554");
            socket_send.send((zmq::message_t &) "end", sendFlags);
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