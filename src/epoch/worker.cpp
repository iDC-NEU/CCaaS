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

    void StateChecker(const Context& ctx) {
        SetCPU();
        MessageSendHandler sendHandler;
        MessageReceiveHandler receiveHandler;
        receiveHandler.Init(ctx, 0);
        bool sleep_flag;
        while(!EpochManager::IsInitOK()) usleep(1000);
//        printf("State Checker\n");
        while (!EpochManager::IsTimerStop()) {
            sleep_flag = false;
            sleep_flag = receiveHandler.CheckReceivedStatesAndReply() | sleep_flag;/// check and send ack
            sleep_flag = MessageSendHandler::SendEpochEndMessage(ctx) | sleep_flag;///send epoch end flag
            sleep_flag = MessageSendHandler::SendBackUpEpochEndMessage(ctx) | sleep_flag;///send epoch backup end message
            sleep_flag = MessageSendHandler::SendAbortSet(ctx) | sleep_flag; ///send abort set
            if(!sleep_flag) usleep(100);
        }
    }

    void WorkerFroMessageThreadMain(const Context& ctx, uint64_t id) {
        MessageReceiveHandler receiveHandler;
        receiveHandler.Init(ctx, id);
        while(!EpochManager::IsInitOK()) usleep(1000);
        while(!EpochManager::IsTimerStop()) {
            receiveHandler.HandleReceivedMessage_Block();
        }
    }

    void WorkerFroCommitThreadMain(const Context& ctx, uint64_t id) {
        Merger merger;
        merger.Init(ctx, id);
        auto epoch = EpochManager::GetLogicalEpoch();
        auto txn_ptr = std::make_unique<proto::Transaction>();
        while(!EpochManager::IsInitOK()) usleep(1000);
        if(id == 0) {
//            merger.EpochCommit_RedoLog_TxnMode_Commit_Queue();
            while(!EpochManager::IsTimerStop()) {
                epoch = EpochManager::GetLogicalEpoch();
                while(!EpochManager::IsShardingMergeComplete(epoch)) {
                    EpochManager::CheckEpochMergeState();
                    usleep(50);
                }
                while(!EpochManager::IsAbortSetMergeComplete(epoch)) {
                    EpochManager::CheckEpochAbortMergeState();
                    usleep(50);
                }
                merger.EpochCommit_RedoLog_TxnMode_Commit_Queue_usleep();
            }
        }
        else {
            merger.EpochCommit_RedoLog_TxnMode_Commit_Queue_Wait();
        }
    }

    void WorkerFroTiKVStorageThreadMain(uint64_t id) {
        uint64_t epoch;
        auto txn_ptr = std::make_unique<proto::Transaction>();
        while(!EpochManager::IsInitOK()) usleep(1000);
        if(id == 0) {
//            TiKV::sendTransactionToTiKV();
            while(!EpochManager::IsTimerStop()) {
                epoch = EpochManager::GetPushDownEpoch();
                while(!EpochManager::IsCommitComplete(epoch)) usleep(50);
                TiKV::sendTransactionToTiKV_usleep();
            }
        }
        else {
            while (!EpochManager::IsTimerStop()) {
                TiKV::sendTransactionToTiKV_Wait();
            }
        }
    }

    void WorkerFroMOTStorageThreadMain(const Context& ctx) {
        MOT::SendToMOThreadMain_usleep();
//        MOT::SendToMOThreadMain();
    }

}

