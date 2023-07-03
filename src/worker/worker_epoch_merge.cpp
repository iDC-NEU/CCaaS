//
// Created by 周慰星 on 2022/9/14.
//
#include "worker/worker_epoch_merge.h"
#include "epoch/epoch_manager.h"
#include "message/message.h"
#include "transaction/merge.h"
#include "storage/tikv.h"
#include "storage/mot.h"

namespace Taas {

    void WorkerFroEpochMessageThreadMain(const Context& ctx, uint64_t id) {/// handle epoch end message
        std::string name = "EpochMessage-" + std::to_string(id);
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        MessageReceiveHandler receiveHandler;
        receiveHandler.Init(ctx, id);
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()) {
            receiveHandler.HandleReceivedEpochMessage_Usleep();
//            receiveHandler.HandleReceivedEpochMessage_Block();
        }
    }

    void WorkerFroTxnMessageThreadMain(const Context& ctx, uint64_t id) {/// handle client txn and remote server txn
        std::string name = "EpochTxnMessage-" + std::to_string(id);
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        MessageReceiveHandler receiveHandler;
        receiveHandler.Init(ctx, id);
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()) {
            receiveHandler.HandleReceivedTxnMessage_Usleep();
//            receiveHandler.HandleReceivedTxnMessage_Block();
        }
    }

    void WorkerFroMergeThreadMain(const Context& ctx, uint64_t id) {
        std::string name = "EpochMerge-" + std::to_string(id);
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        Merger merger;
        merger.Init(ctx, id);
        uint64_t epoch;
        auto txn_ptr = std::make_unique<proto::Transaction>();
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        if(id == 0) {
            while(!EpochManager::IsTimerStop()) {
                epoch = EpochManager::GetLogicalEpoch();
                while(!EpochManager::IsAbortSetMergeComplete(epoch)) {
                    usleep(sleep_time);
                }
                merger.EpochMerge_MergeQueue_Usleep();
            }
        }
        else {
            while(!EpochManager::IsTimerStop()) {
                merger.EpochMerge_MergeQueue_Usleep();
            }
        }
    }

    void WorkerFroCommitThreadMain(const Context& ctx, uint64_t id) {
        std::string name = "EpochCommit-" + std::to_string(id);
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        Merger merger;
        merger.Init(ctx, id);
        uint64_t  epoch;
        auto txn_ptr = std::make_unique<proto::Transaction>();
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        if(id == 0) {
            while(!EpochManager::IsTimerStop()) {
                epoch = EpochManager::GetLogicalEpoch();
                while(!EpochManager::IsAbortSetMergeComplete(epoch)) {
                    usleep(sleep_time);
                }
//                merger.EpochCommit_CommitQueue();
                merger.EpochCommit_EpochLocalTxnQueue_Usleep();
            }
        }
        else {
            while(!EpochManager::IsTimerStop()) {
                merger.EpochCommit_EpochLocalTxnQueue_Usleep();
//                merger.EpochCommit_CommitQueue_Block();
            }
        }
    }


}

