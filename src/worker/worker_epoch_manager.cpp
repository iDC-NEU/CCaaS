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
        switch(ctx.taasContext.taasMode) {
            case TaasMode::MultiModel :
            case TaasMode::MultiMaster : {
                EpochPhysicalTimerManagerThreadMain(ctx);
                break;
            }
            case TaasMode::Sharding : {
                EpochPhysicalTimerManagerThreadMain(ctx);
                break;
            }
            case TaasMode::TwoPC : {
                EpochPhysicalTimerManagerThreadMain(ctx);
//                MultiMasterEpochManager::EpochLogicalTimerManagerThreadMain(ctx);
            }
        }
//        EpochPhysicalTimerManagerThreadMain(ctx);
        return ;

    }

    void WorkerForLogicalThreadMain(const Context& ctx) {
        std::string name = "EpochLogical";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        SetCPU();
        switch(ctx.taasContext.taasMode) {
            case TaasMode::MultiModel :
            case TaasMode::MultiMaster : {
                MultiMasterEpochManager::EpochLogicalTimerManagerThreadMain(ctx);
                break;
            }
            case TaasMode::Sharding : {
                ShardingEpochManager::EpochLogicalTimerManagerThreadMain(ctx);
                break;
            }
            case TaasMode::TwoPC : {
                MultiMasterEpochManager::EpochLogicalTimerManagerThreadMain(ctx);
            }
        }
        return ;
    }

    void WorkerForLogicalRedoLogPushDownCheckThreadMain(const Context& ctx) {
        SetCPU();
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){
            CheckRedoLogPushDownState(ctx);
        }
    }

}

