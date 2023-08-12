//
// Created by 周慰星 on 2022/9/14.
//
#include "worker/worker_message.h"
#include "epoch/epoch_manager.h"
#include "message/message.h"
#include "transaction/merge.h"
#include "transaction/two_phase_commit.h"
#include "storage/tikv.h"
#include "storage/mot.h"

namespace Taas {

    void WorkerFroMessageThreadMain(const Context& ctx, uint64_t id) {/// handle client txn
        std::string name = "TxnMessage-" + std::to_string(id);
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        EpochMessageReceiveHandler receiveHandler;
        class TwoPC twoPC;
        receiveHandler.Init(ctx, id);
        twoPC.Init(ctx, id);
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){
            switch(ctx.taas_mode) {
                case TaasMode::MultiMaster :
                case TaasMode::Sharding : {
                    while(!EpochManager::IsTimerStop()) {
                        receiveHandler.HandleReceivedMessage();
                    }
                    break;
                }
                case TaasMode::TwoPC : {
                    while(!EpochManager::IsTimerStop()) {
                        twoPC.HandleReceivedMessage();
                    }
                    break;
                }
            }
        }
    }

    void WorkerFroMessageEpochThreadMain(const Context& ctx, uint64_t id) {/// handle message
        std::string name = "EpochMessage-" + std::to_string(id);
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        EpochMessageReceiveHandler receiveHandler;
        class TwoPC twoPC;
        receiveHandler.Init(ctx, id);
        twoPC.Init(ctx, id);
        if(id < 2) SetCPU();
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){
            switch(ctx.taas_mode) {
                case TaasMode::MultiMaster :
                case TaasMode::Sharding : {
                    while(!EpochManager::IsTimerStop()) {
                        receiveHandler.HandleReceivedControlMessage();
                    }
                    break;
                }
                case TaasMode::TwoPC : {
                    while(!EpochManager::IsTimerStop()) {
//                        twoPC.HandleReceivedMessage();
                    }
                    break;
                }
            }
        }
    }

    void WorkerForClientListenThreadMain(const Context& ctx) {
        std::string name = "EpochClientListen";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        SetCPU();
        ListenClientThreadMain(ctx);
    }

    void WorkerForClientSendThreadMain(const Context& ctx) {
        std::string name = "EpochClientSend";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        SetCPU();
        SendClientThreadMain(ctx);
    }

    void WorkerForServerListenThreadMain(const Context& ctx) {
        std::string name = "EpochServerListen";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        SetCPU();
        ListenServerThreadMain(ctx);
    }

    void WorkerForServerListenThreadMain_Epoch(const Context& ctx) {
        std::string name = "EpochServerListen";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        SetCPU();
        ListenServerThreadMain_Epoch(ctx);
    }

    void WorkerForServerSendThreadMain(const Context& ctx) {
        std::string name = "EpochServerSend";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        SetCPU();
        SendServerThreadMain(ctx);
    }

    void WorkerForStorageSendThreadMain(const Context& ctx) {
        std::string name = "EpochStorageSend";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        SetCPU();
        SendStoragePUBThreadMain(ctx);
    }

}

