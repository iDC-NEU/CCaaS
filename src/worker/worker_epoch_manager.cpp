//
// Created by 周慰星 on 2022/9/14.
//
#include "worker/worker_epoch_manager.h"
#include "epoch/epoch_manager_sharding.h"
#include "epoch/epoch_manager_multi_master.h"
#include "epoch/epoch_manager.h"
#include "message/message.h"
#include "transaction/merge.h"
#include "storage/tikv.h"
#include "storage/mot.h"

namespace Taas {

    void WorkerForPhysicalThreadMain(const Context &ctx) {
        std::string name = "EpochPhysical";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        SetCPU();
        EpochPhysicalTimerManagerThreadMain(ctx);
    }

    void WorkerForLogicalThreadMain(const Context& ctx) {
        SetCPU();
        switch(ctx.taas_mode) {
            case TaasMode::MultiMaster :
            case TaasMode::Sharding : {
                EpochLogicalTimerManagerThreadMain(ctx);
                break;
            }
            case TaasMode::TwoPC : {

                goto end;
            }
        }
        end:
        return ;
    }

    void WorkerForLogicalTxnMergeCheckThreadMain(const Context& ctx) {
        std::string name = "EpochTxnMergeCheck";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        SetCPU();
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){
            switch(ctx.taas_mode) {
                case TaasMode::MultiMaster : {
                    while(EpochManager::GetPhysicalEpoch() <= EpochManager::GetLogicalEpoch() + ctx.kDelayEpochNum ||
                          !MultiMasterEpochManager::CheckEpochMergeState(ctx)) {
                        usleep(logical_sleep_timme);
                    }
                    break;
                }
                case TaasMode::Sharding : {
                    while(EpochManager::GetPhysicalEpoch() <= EpochManager::GetLogicalEpoch() + ctx.kDelayEpochNum ||
                          !ShardingEpochManager::CheckEpochMergeState(ctx)) {
                        usleep(logical_sleep_timme);
                    }
                    break;
                }
                case TaasMode::TwoPC : {
                    goto end;
                }
            }

        }
        end:
        return ;
    }

    void WorkerForLogicalAbortSetMergeCheckThreadMain(const Context& ctx) {
        std::string name = "EpochAbortSetMergeCheck";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        SetCPU();
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){
            switch(ctx.taas_mode) {
                case TaasMode::MultiMaster : {
                    while(!MultiMasterEpochManager::CheckEpochAbortMergeState(ctx)) {
                        usleep(logical_sleep_timme);
                    }
                    break;
                }
                case TaasMode::Sharding : {
                    while(!ShardingEpochManager::CheckEpochAbortMergeState(ctx)) {
                        usleep(logical_sleep_timme);
                    }
                    break;
                }
                case TaasMode::TwoPC : {
                    goto end;
                }
            }

        }
        end:
        return ;
    }

    void WorkerForLogicalCommitCheckThreadMain(const Context& ctx) {
        std::string name = "EpochCommitCheck";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        SetCPU();
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){
            switch(ctx.taas_mode) {
                case TaasMode::MultiMaster : {
                    while(!MultiMasterEpochManager::CheckEpochCommitState(ctx)) {
                        usleep(logical_sleep_timme);
                    }
                    break;
                }
                case TaasMode::Sharding : {
                    while(!ShardingEpochManager::CheckEpochCommitState(ctx)) {
                        usleep(logical_sleep_timme);
                    }
                    break;
                }
                case TaasMode::TwoPC : {
                    goto end;
                }
            }

        }
        end:
        return ;
    }

    void WorkerForLogicalReceiveAndReplyCheckThreadMain(const Context& ctx) {
        SetCPU();
        bool sleep_flag;
        MessageReceiveHandler receiveHandler;
        receiveHandler.Init(ctx, 0);
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){
            switch(ctx.taas_mode) {
                case TaasMode::MultiMaster :
                case TaasMode::Sharding :{
                    while(!EpochManager::IsTimerStop()) {
                        sleep_flag = receiveHandler.CheckReceivedStatesAndReply();/// check and send EpochTxnACK BackUpACK ack /// check and send EpochLogPushDownComplete ack
                        if(!sleep_flag) usleep(sleep_time);
                    }
                    break;
                }
                case TaasMode::TwoPC : {
                    goto end;
                }
            }

        }
        end:
        return ;
    }

    void WorkerForEpochAbortSendThreadMain(const Context& ctx) {
        SetCPU();
        bool sleep_flag;
        MessageReceiveHandler receiveHandler;
        receiveHandler.Init(ctx, 0);
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){
            switch(ctx.taas_mode) {
                case TaasMode::MultiMaster :
                case TaasMode::TwoPC :{
                    goto end;
                }
                case TaasMode::Sharding : {
                    while(!EpochManager::IsTimerStop()) {
                        sleep_flag = MessageSendHandler::SendAbortSet(ctx); ///check and send abort set
                        if(!sleep_flag) usleep(sleep_time);
                    }
                    break;
                }
            }
        }
        end:
        return ;
    }

    void WorkerForEpochEndFlagSendThreadMain(const Context& ctx) {
        SetCPU();
        bool sleep_flag;
        MessageReceiveHandler receiveHandler;
        receiveHandler.Init(ctx, 0);
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){
            switch(ctx.taas_mode) {
                case TaasMode::MultiMaster :
                case TaasMode::Sharding :{
                    while(!EpochManager::IsTimerStop()) {
                        sleep_flag = MessageSendHandler::SendEpochEndMessage(ctx); ///send epoch end flag
                        if(!sleep_flag) usleep(sleep_time);
                    }
                    break;
                }
                case TaasMode::TwoPC : {
                    goto end;
                }
            }
        }
        end:
        return ;
    }

    void WorkerForEpochBackUpEndFlagSendThreadMain(const Context& ctx) {
        SetCPU();
        bool sleep_flag;
        MessageReceiveHandler receiveHandler;
        receiveHandler.Init(ctx, 0);
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){
            switch(ctx.taas_mode) {
                case TaasMode::MultiMaster :
                case TaasMode::Sharding :{
                    while(!EpochManager::IsTimerStop()) {
                        sleep_flag = MessageSendHandler::SendBackUpEpochEndMessage(ctx); ///send epoch backup end message
                        if(!sleep_flag) usleep(sleep_time);
                    }
                    break;
                }
                case TaasMode::TwoPC : {
                    goto end;
                }
            }
        }
        end:
        return ;
    }

    void WorkerForLogicalRedoLogPushDownCheckThreadMain(const Context& ctx) {
        SetCPU();
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){
            while(EpochManager::GetPhysicalEpoch() <= EpochManager::GetLogicalEpoch() + ctx.kDelayEpochNum ||
                  !CheckRedoLogPushDownState(ctx)) { //this check do in mot push down
                usleep(logical_sleep_timme);
            }
        }
    }

}

