//
// Created by 周慰星 on 2022/9/14.
//
#include "epoch/worker.h"
#include "epoch/epoch_manager.h"
#include "message/message.h"
#include "transaction/merge.h"
#include "storage/tikv.h"
#include "storage/mot.h"

namespace Taas {

/**
 * @brief do local_merge remote_merge and commit
 *
 * @param ctx XML中的配置相关信息
 * @return true
 * @return false
 */

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
        std::string name = "EpochTCommitCheck";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        SetCPU();
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){
            while(!EpochManager::CheckEpochCommitState()) {
                usleep(logical_sleep_timme);
            }
        }
    }

    void WorkerFroEpochMessageThreadMain(const Context& ctx, uint64_t id) {/// handle epoch end message
        std::string name = "EpochMessage-" + std::to_string(id);
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        MessageReceiveHandler receiveHandler;
        receiveHandler.Init(ctx, id);
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        if(id < 3) {
            SetCPU();
            bool sleep_flag;
            while(!EpochManager::IsTimerStop()) {
                sleep_flag = receiveHandler.HandleReceivedEpochMessage();
                if(!sleep_flag) usleep(sleep_time);
            }
        }
        while(!EpochManager::IsTimerStop()) {
            receiveHandler.HandleReceivedEpochMessage_Block();
        }
    }

    void WorkerFroTxnMessageThreadMain(const Context& ctx, uint64_t id) {/// handle client txn and remote server txn
        std::string name = "EpochTxnMessage-" + std::to_string(id);
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        MessageReceiveHandler receiveHandler;
        receiveHandler.Init(ctx, id);
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        if(id == 0) {
            SetCPU();
            bool sleep_flag;
            while(!EpochManager::IsTimerStop()) {
                sleep_flag = receiveHandler.HandleReceivedTxnMessage();
                sleep_flag = receiveHandler.CheckReceivedStatesAndReply() | sleep_flag; /// check and send EpochShardingACK BackUpACK ack /// check and send EpochLogPushDownComplete ack
                if(!sleep_flag) usleep(sleep_time);
            }
        }
        else if(id == 1) {
            SetCPU();
            bool sleep_flag;
            while(!EpochManager::IsTimerStop()) {
                sleep_flag = receiveHandler.HandleReceivedTxnMessage();
                sleep_flag = MessageSendHandler::SendEpochEndMessage(ctx)| sleep_flag; ///check and send abort set
                sleep_flag = MessageSendHandler::SendBackUpEpochEndMessage(ctx)| sleep_flag; ///check and send abort set
                if(!sleep_flag) usleep(sleep_time);
            }
        }
        while(!EpochManager::IsTimerStop()) {
            receiveHandler.HandleReceivedTxnMessage_Block();
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
            SetCPU();
            while(!EpochManager::IsTimerStop()) {
                epoch = EpochManager::GetLogicalEpoch();
                while(!EpochManager::IsAbortSetMergeComplete(epoch)) {
                    usleep(sleep_time);
                    MessageSendHandler::SendAbortSet(ctx); ///check and send abort set
                }
                merger.EpochCommit_RedoLog_TxnMode_Commit_Queue();
            }
        }
        else {
            while(!EpochManager::IsTimerStop()) {
                merger.EpochCommit_RedoLog_TxnMode_Commit_Queue_usleep();
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

    void WorkerFroTiKVStorageThreadMain(const Context& ctx, uint64_t id) {
        std::string name = "EpochTikv-" + std::to_string(id);
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        uint64_t epoch;
        MessageReceiveHandler receiveHandler;
        receiveHandler.Init(ctx, id);
        auto txn_ptr = std::make_unique<proto::Transaction>();
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        if(id < 5) {
            while(!EpochManager::IsTimerStop()) {
                epoch = EpochManager::GetPushDownEpoch();
                while(!EpochManager::IsCommitComplete(epoch)) {
                    usleep(sleep_time);
                }
                TiKV::sendTransactionToTiKV_usleep();
            }
        }
        else {
            while (!EpochManager::IsTimerStop()) {
                TiKV::sendTransactionToTiKV_usleep1();
                usleep(sleep_time);
            }
        }
    }

    void WorkerFroMOTStorageThreadMain() {
        std::string name = "EpochMOT";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
        SetCPU();
        MOT::SendToMOThreadMain_usleep(); ///EpochManager::CheckRedoLogPushDownState(); in this function
//        MOT::SendToMOThreadMain();
    }









































    void WorkerForLogicalRedoLogPushDownCheckThreadMain(const Context& ctx) {
        SetCPU();
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        while(!EpochManager::IsTimerStop()){
            while(EpochManager::GetPhysicalEpoch() <= EpochManager::GetLogicalEpoch() + ctx.kDelayEpochNum ||
                  !EpochManager::CheckRedoLogPushDownState()) {
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

}

