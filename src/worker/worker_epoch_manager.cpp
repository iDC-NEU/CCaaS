//
// Created by 周慰星 on 2022/9/14.
//
#include "worker/worker_epoch_manager.h"
#include "epoch/epoch_manager_sharding.h"
#include "epoch/epoch_manager_multi_master.h"
#include "epoch/epoch_manager.h"
#include "transaction/merge.h"

namespace Taas {

    void WorkerForPhysicalThreadMain(const Context &ctx) {
        std::string name = "EpochPhysical";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        SetCPU();
        EpochPhysicalTimerManagerThreadMain(ctx);
    }

    void WorkerForLogicalThreadMain(const Context& ctx) {
        std::string name = "EpochLogical";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        SetCPU();
        switch(ctx.taas_mode) {
            case TaasMode::MultiMaster : {
                MultiMasterEpochManager::EpochLogicalTimerManagerThreadMain(ctx);
                break;
            }
            case TaasMode::Sharding : {
                ShardingEpochManager::EpochLogicalTimerManagerThreadMain(ctx);
                break;
            }
            case TaasMode::TwoPC : {
                goto end;
            }
        }
        end:
        return ;
    }
}

