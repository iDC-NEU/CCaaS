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
        auto txn_ptr = std::make_shared<proto::Transaction>();
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        switch(ctx.taas_mode) {
            case TaasMode::MultiMaster :
            case TaasMode::Sharding : {
                while(!EpochManager::IsTimerStop()) {
//                    while(!EpochManager::IsTimerStop())
//                        merger.EpochMerge_Usleep();
                    if(id == 0)
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
    }

    void WorkerFroCommitThreadMain(const Context& ctx, uint64_t id) {
        std::string name = "EpochCommit-" + std::to_string(id);
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        Merger merger;
        merger.Init(ctx, id);
        auto txn_ptr = std::make_shared<proto::Transaction>();
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        switch (ctx.taas_mode) {
            case TaasMode::MultiMaster :
            case TaasMode::Sharding : {
//                while (!EpochManager::IsTimerStop())
//                    merger.EpochCommit_Usleep();
                    if (id == 0)
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
    }

}

