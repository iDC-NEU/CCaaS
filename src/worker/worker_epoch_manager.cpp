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

    void WorkerForLogicalTxnMergeCheckThreadMain(const Context& ctx) {
        std::string name = "EpochTxnMergeCheck";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){
            switch(ctx.taas_mode) {
                case TaasMode::MultiMaster : {
                    while(!EpochManager::IsTimerStop()) {
                        while (EpochManager::GetPhysicalEpoch() <=
                               EpochManager::GetLogicalEpoch() + ctx.kDelayEpochNum ||
                               !MultiMasterEpochManager::CheckEpochMergeState(ctx)) {
                            usleep(logical_sleep_timme);
                        }
                    }
                    break;
                }
                case TaasMode::Sharding : {
                    while(!EpochManager::IsTimerStop()) {
                        while (EpochManager::GetPhysicalEpoch() <=
                               EpochManager::GetLogicalEpoch() + ctx.kDelayEpochNum ||
                               !ShardingEpochManager::CheckEpochMergeState(ctx)) {
                            usleep(logical_sleep_timme);
                        }
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
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){
            switch(ctx.taas_mode) {
                case TaasMode::MultiMaster : {
                    while(!EpochManager::IsTimerStop()) {
                        while (!MultiMasterEpochManager::CheckEpochAbortMergeState(ctx)) {
                            usleep(logical_sleep_timme);
                        }
                    }
                    break;
                }
                case TaasMode::Sharding : {
                    while(!EpochManager::IsTimerStop()) {
                        while(!ShardingEpochManager::CheckEpochAbortMergeState(ctx)) {
                            usleep(logical_sleep_timme);
                        }
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
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){
            switch(ctx.taas_mode) {
                case TaasMode::MultiMaster : {
                    while(!EpochManager::IsTimerStop()) {
                        while (!MultiMasterEpochManager::CheckEpochCommitState(ctx)) {
                            usleep(logical_sleep_timme);
                        }
                    }
                    break;
                }
                case TaasMode::Sharding : {
                    while(!EpochManager::IsTimerStop()) {
                        while (!ShardingEpochManager::CheckEpochCommitState(ctx)) {
                            usleep(logical_sleep_timme);
                        }
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

    void WorkerForEpochControlMessageThreadMain(const Context& ctx) {
        EpochMessageReceiveHandler receiveHandler;
        receiveHandler.Init(ctx, 0);
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){
            switch(ctx.taas_mode) {
                case TaasMode::MultiMaster :
                case TaasMode::Sharding :{
                    while(!EpochManager::IsTimerStop()) {
                        ///check and send abort set
                        /// send epoch end flag
                        /// send epoch backup end message
                        while(!EpochMessageSendHandler::SendEpochControlMessage(ctx, receiveHandler)) {
                            usleep(logical_sleep_timme);
                        }
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
        EpochMessageReceiveHandler receiveHandler;
        receiveHandler.Init(ctx, 0);
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){
            switch(ctx.taas_mode) {
                case TaasMode::MultiMaster :
                case TaasMode::Sharding :{
                    while(!EpochManager::IsTimerStop()) {
                        while(!receiveHandler.CheckReceivedStatesAndReply()) {
                            usleep(logical_sleep_timme);
                        }
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
        EpochMessageReceiveHandler receiveHandler;
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
                        while(!EpochMessageSendHandler::SendAbortSet(ctx)) { ///check and send abort set
                            usleep(logical_sleep_timme);
                        }
                    }
                    break;
                }
            }
        }
        end:
        return ;
    }

    void WorkerForEpochEndFlagSendThreadMain(const Context& ctx) {
        EpochMessageReceiveHandler receiveHandler;
        receiveHandler.Init(ctx, 0);
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){
            switch(ctx.taas_mode) {
                case TaasMode::MultiMaster :
                case TaasMode::Sharding :{
                    while(!EpochManager::IsTimerStop()) {
                        while(!EpochMessageSendHandler::SendEpochEndMessage(ctx)) {
                            usleep(logical_sleep_timme);
                        }
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
        EpochMessageReceiveHandler receiveHandler;
        receiveHandler.Init(ctx, 0);
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){
            switch(ctx.taas_mode) {
                case TaasMode::MultiMaster :
                case TaasMode::Sharding :{
                    while(!EpochManager::IsTimerStop()) {
                        while(! EpochMessageSendHandler::SendBackUpEpochEndMessage(ctx)) { ///send epoch backup end message
                            usleep(logical_sleep_timme);
                        }
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
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){
            CheckRedoLogPushDownState(ctx);
        }
    }

}

