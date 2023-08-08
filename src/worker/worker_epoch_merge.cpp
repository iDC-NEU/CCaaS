//
// Created by 周慰星 on 2022/9/14.
//
#include "worker/worker_epoch_merge.h"
#include "epoch/epoch_manager.h"
#include "transaction/merge.h"
#include "storage/mot.h"

namespace Taas {

    void WorkerFroMergeThreadMain(const Context& ctx, uint64_t id) {
        std::string name = "EpochMerge-" + std::to_string(id);
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        Merger merger;
        merger.Init(ctx, id);
//        uint64_t epoch;
        uint64_t epoch_mod;
        auto txn_ptr = std::make_unique<proto::Transaction>();
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
//        if(id < ctx.kUsleepThreadNum ) {
//            while(!EpochManager::IsTimerStop()){
////                epoch = EpochManager::GetLogicalEpoch();
//                epoch_mod = EpochManager::GetLogicalEpoch() % ctx.kCacheMaxLength;
//                while (Merger::epoch_merge_queue[epoch_mod]->try_dequeue(txn_ptr)) {
////                    if (txn_ptr == nullptr || txn_ptr->txn_type() == proto::TxnType::NullMark) {
////                        continue;
////                    }
//                    Merger::merge_queue->enqueue(std::move(txn_ptr));
//                    Merger::merge_queue->enqueue(nullptr);
//                }
//                usleep(logical_sleep_timme);
//            }
//        }
//        else {
            switch(ctx.taas_mode) {
                case TaasMode::MultiMaster :
                case TaasMode::Sharding : {
                    while(!EpochManager::IsTimerStop()) {
                        while(!EpochManager::IsTimerStop())
                            merger.EpochMerge_Usleep();
                        if(id < ctx.kUsleepThreadNum)
                            while(!EpochManager::IsTimerStop())
                                merger.EpochMerge_Usleep();
                        else
                            while(!EpochManager::IsTimerStop())
                                merger.EpochMerge_Block();
                    }
                    break;
                }
                case TaasMode::TwoPC : {
                    break;
                }
            }
//        }
    }

    void WorkerFroCommitThreadMain(const Context& ctx, uint64_t id) {
        std::string name = "EpochCommit-" + std::to_string(id);
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        Merger merger;
        merger.Init(ctx, id);
        //        uint64_t epoch;
        uint64_t epoch_mod;
        auto txn_ptr = std::make_unique<proto::Transaction>();
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
//        if(id < ctx.kUsleepThreadNum ) {
//            while(!EpochManager::IsTimerStop()){
////                epoch = EpochManager::GetLogicalEpoch();
//                epoch_mod = EpochManager::GetLogicalEpoch() % ctx.kCacheMaxLength;
//                while(!EpochManager::IsAbortSetMergeComplete(epoch_mod)) usleep(logical_sleep_timme);
//                while(Merger::epoch_commit_queue[epoch_mod]->try_dequeue(txn_ptr)) {
//                    Merger::commit_queue->enqueue(std::move(txn_ptr));
//                    Merger::commit_queue->enqueue(nullptr);
//                }
//                usleep(logical_sleep_timme);
//            }
//        }
//        else {
            switch (ctx.taas_mode) {
                case TaasMode::MultiMaster :
                case TaasMode::Sharding : {
                    while (!EpochManager::IsTimerStop())
                        merger.EpochCommit_Usleep();

                    if (id < ctx.kUsleepThreadNum)
                        while (!EpochManager::IsTimerStop())
                            merger.EpochCommit_Usleep();
                    else
                        while (!EpochManager::IsTimerStop())
                            merger.EpochCommit_Block();
                    break;
                }
                case TaasMode::TwoPC : {
                    break;
                }
            }
//        }
    }

}

