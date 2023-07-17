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
        uint64_t epoch;
        auto txn_ptr = std::make_unique<proto::Transaction>();
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){
            switch(ctx.taas_mode) {
                case TaasMode::MultiMaster :
                case TaasMode::Sharding : {
                    while(!EpochManager::IsTimerStop()) {
                        if(id < ctx.kUsleepThreadNum)
                            merger.EpochMerge_Usleep();
                        else
                            merger.EpochMerge_Block();
                    }
                    break;
                }
                case TaasMode::TwoPC : {
                    break;
                }
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
        switch(ctx.taas_mode) {
            case TaasMode::MultiMaster :
            case TaasMode::Sharding : {
                while(!EpochManager::IsTimerStop()) {
                    if(id < ctx.kUsleepThreadNum)
                        merger.EpochCommit_Usleep();
                    else
                        merger.EpochCommit_Block();
                }
                break;
            }
            case TaasMode::TwoPC : {
                break;
            }
        }
    }

}

