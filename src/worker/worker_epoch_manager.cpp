//
// Created by 周慰星 on 2022/9/14.
//
#include "worker/worker_epoch_manager.h"
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
        EpochLogicalTimerManagerThreadMain(ctx);
    }

    void WorkerForLogicalTxnMergeCheckThreadMain(const Context& ctx) {
        std::string name = "EpochTxnMergeCheck";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        SetCPU();
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){
            while(EpochManager::GetPhysicalEpoch() <= EpochManager::GetLogicalEpoch() + ctx.kDelayEpochNum ||
                    !EpochManager::CheckEpochMergeState()) {
                usleep(logical_sleep_timme);
            }
        }
    }

    void WorkerForLogicalAbortSetMergeCheckThreadMain() {
        std::string name = "EpochAbortSetMergeCheck";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        SetCPU();
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){
            while(!EpochManager::CheckEpochAbortMergeState()) {
                usleep(logical_sleep_timme);
            }
        }
    }

    void WorkerForLogicalCommitCheckThreadMain() {
        std::string name = "EpochCommitCheck";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        SetCPU();
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){
            while(!EpochManager::CheckEpochCommitState()) {
                usleep(logical_sleep_timme);
            }
        }
    }

    void WorkerForLogicalReceiveAndReplyCheckThreadMain(const Context& ctx) {
        SetCPU();
        bool sleep_flag;
        MessageReceiveHandler receiveHandler;
        receiveHandler.Init(ctx, 0);
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){
            sleep_flag = receiveHandler.CheckReceivedStatesAndReply();/// check and send EpochShardingACK BackUpACK ack /// check and send EpochLogPushDownComplete ack
            if(!sleep_flag) usleep(sleep_time);
        }
    }

    void WorkerForEpochAbortSendThreadMain(const Context& ctx) {
        SetCPU();
        bool sleep_flag;
        MessageReceiveHandler receiveHandler;
        receiveHandler.Init(ctx, 0);
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){
            sleep_flag = MessageSendHandler::SendAbortSet(ctx); ///check and send abort set
            if(!sleep_flag) usleep(sleep_time);
        }
    }

    void WorkerForEpochEndFlagSendThreadMain(const Context& ctx) {
        SetCPU();
        bool sleep_flag;
        MessageReceiveHandler receiveHandler;
        receiveHandler.Init(ctx, 0);
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){
            sleep_flag = MessageSendHandler::SendEpochEndMessage(ctx); ///send epoch end flag
            if(!sleep_flag) usleep(sleep_time);
        }
    }

    void WorkerForEpochBackUpEndFlagSendThreadMain(const Context& ctx) {
        SetCPU();
        bool sleep_flag;
        MessageReceiveHandler receiveHandler;
        receiveHandler.Init(ctx, 0);
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){
            sleep_flag = MessageSendHandler::SendBackUpEpochEndMessage(ctx); ///send epoch backup end message
            if(!sleep_flag) usleep(sleep_time);
        }
    }

    void WorkerForLogicalRedoLogPushDownCheckThreadMain(const Context& ctx) {
        SetCPU();
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){
            while(EpochManager::GetPhysicalEpoch() <= EpochManager::GetLogicalEpoch() + ctx.kDelayEpochNum ||
                  !EpochManager::CheckRedoLogPushDownState()) { //this check do in mot push down
                usleep(logical_sleep_timme);
            }
        }
    }

}

