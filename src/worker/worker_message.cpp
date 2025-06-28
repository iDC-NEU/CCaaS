//
// Created by 周慰星 on 2022/9/14.
//
#include "worker/worker_message.h"
#include "epoch/epoch_manager.h"
#include "message/message.h"
#include "transaction/merge.h"
#include "transaction/two_phase_commit.h"

namespace Taas {

    void WorkerFroMessageThreadMain(uint64_t id) {/// handle client txn
        std::string name = "TxnMessage-" + std::to_string(id);
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        EpochMessageReceiveHandler receiveHandler;
        class TwoPC twoPC;
        while(init_ok_num.load() < 5) usleep(sleep_time);
        receiveHandler.Init(id);
        Taas::TwoPC::Init(id);
        init_ok_num.fetch_add(1);
//        bool sleep_flag;
//        auto safe_length = TaasContext::kCacheMaxLength / 10;
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){
            switch(TaasContext::taasMode) {
                case TaasMode::MultiModel :
                case TaasMode::MultiMaster :
                case TaasMode::Shard : {
                    while(!EpochManager::IsTimerStop()) {
//                        sleep_flag = true;
//                        receiveHandler.TryHandleReceivedControlMessage();
//                        if( EpochManager::GetLogicalEpoch() + safe_length > EpochManager::GetPhysicalEpoch() ) /// avoid task backlogs, stop handling txn comes from the client
//                            receiveHandler.TryHandleReceivedMessage();
//
//                        sleep_flag = sleep_flag & receiveHandler.sleep_flag;
//
//                        if(sleep_flag) usleep(merge_sleep_time);
                        while(!EpochManager::IsTimerStop())
                            receiveHandler.HandleReceivedMessage();
                    }
                    break;
                }
                case TaasMode::TwoPC : {
                    while(!EpochManager::IsTimerStop()) {
                        twoPC.HandleClientMessage();        // test
//                        twoPC.HandleReceivedMessage();

                    }
                    break;
                }
            }
        }
    }

    void WorkerFroMessageEpochThreadMain(uint64_t id) {/// handle message
        std::string name = "EpochMessage-" + std::to_string(id);
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        EpochMessageReceiveHandler receiveHandler;
        class TwoPC twoPC;
        while(init_ok_num.load() < 5) usleep(sleep_time);
        receiveHandler.Init(id);
        Taas::TwoPC::Init(id);
        init_ok_num.fetch_add(1);
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){
            switch(TaasContext::taasMode) {
                case TaasMode::MultiModel :
                case TaasMode::MultiMaster :
                case TaasMode::Shard : {
                    while(!EpochManager::IsTimerStop()) {
                        receiveHandler.HandleReceivedControlMessage();
                    }
                    break;
                }
                case TaasMode::TwoPC : {
                    while(!EpochManager::IsTimerStop()) {
                        twoPC.HandleReceivedMessage();      // test
                    }
                    break;
                }
            }
        }
    }

    void WorkerForClientListenThreadMain() {
        std::string name = "EpochClientListen";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        SetCPU();
        ListenClientThreadMain();
    }

    void WorkerForClientSendThreadMain() {
        std::string name = "EpochClientSend";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        SetCPU();
        SendClientThreadMain();
    }

    void WorkerForServerListenThreadMain() {
        std::string name = "EpochServerListen";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        SetCPU();
        ListenServerThreadMain();
    }

    void WorkerForServerListenThreadMain_Epoch() {
        std::string name = "EpochServerListen";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        SetCPU();
        ListenServerThreadMain_Sub();
    }

    void WorkerForServerSendThreadMain() {
        std::string name = "EpochServerSend";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        SetCPU();
        SendServerThreadMain();
    }

    void WorkerForServerSendPUBThreadMain() {
        std::string name = "EpochClientSend";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        SetCPU();
        SendServerPUBThreadMain();
    }

    void WorkerForStorageSendMOTThreadMain() {
        std::string name = "EpochMOTStorage";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        SendToMOTStorageThreadMain();
    }

    void WorkerForStorageSendNebulaThreadMain() {
        std::string name = "EpochNebulaStorage";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        SendToNebulaStorageThreadMain();
    }
}

