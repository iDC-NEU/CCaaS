//
// Created by 周慰星 on 11/8/22.
//

#include <iostream>
#include <thread>
#include "epoch/epoch_manager.h"
#include "test/test.h"
using namespace std;

namespace Taas {
    int main() {
        Context ctx;
        ctx.GetServerInfo();
        EpochManager epochManager;
        epochManager.ctx = ctx;
        printf("System Start\n");
        std::vector<std::unique_ptr<std::thread>> threads;

        threads.push_back(std::make_unique<std::thread>(EpochPhysicalTimerManagerThreadMain, 0,ctx));
        threads.push_back(std::make_unique<std::thread>(EpochLogicalTimerManagerThreadMain,  0,ctx));

        if(ctx.kTxnNodeNum > 1) {
            threads.push_back(std::make_unique<std::thread>(SendServerThreadMain,  0,ctx));
            threads.push_back(std::make_unique<std::thread>(ListenServerThreadMain,  0,ctx));
        }

        for(int i = 0; i < (int)ctx.kSendClientThreadNum; i ++) {
            threads.push_back(std::make_unique<std::thread>(SendClientThreadMain, i ,ctx));
        }
        threads.push_back(std::make_unique<std::thread>(ListenClientThreadMain,  0,ctx));

//    threads.push_back(std::make_unique<std::thread>(ListenStorageThreadMain,  0,ctx));

//    threads.push_back(std::make_unique<std::thread>(SendStoragePUBThreadMain,  0,ctx));
        threads.push_back(std::make_unique<std::thread>(SendStoragePUBThreadMain2,  0,ctx));

        for(int i = 0; i < (int)ctx.kPackThreadNum; i ++) {
            threads.push_back(std::make_unique<std::thread>(PackThreadMain, i ,ctx));
        }

        for(int i = 0; i < (int)ctx.kMessageCacheThreadNum; i ++) {
            threads.push_back(std::make_unique<std::thread>(MessageCacheThreadMain, i ,ctx));
        }

        for(int i = 0; i < (int)ctx.kWorkerThreadNum; i ++) {
            threads.push_back(std::make_unique<std::thread>(MergeWorkerThreadMain, i ,ctx));
        }

        for(int i = 0; i < (int)ctx.kTestClientNum; i ++) {
            threads.push_back(std::make_unique<std::thread>(Client, i ,ctx));
        }

        if(ctx.is_tikv_enable) {
            EpochManager::tikv_client_ptr = new tikv_client::TransactionClient({ctx.kTiKVIP});
            for(int i = 0; i < (int)ctx.kTiKVSendThreadNum; i ++) {
                threads.push_back(std::make_unique<std::thread>(SendTiKVThreadMain, i ,ctx));
            }
        }


        if(ctx.kDurationTime_us != 0) {
            while(!test_start.load()) usleep(10000);
            usleep(ctx.kDurationTime_us);
            EpochManager::SetTimerStop(true);
            send_to_client_queue.enqueue(nullptr);
            send_to_server_queue.enqueue(nullptr);

            zmq::context_t context(1);
            zmq::socket_t socket_send(context, ZMQ_PUSH);
            socket_send.bind("tcp://localhost:5554");
            socket_send.send((zmq::message_t &) "end");
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