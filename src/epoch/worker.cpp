//
// Created by 周慰星 on 2022/9/14.
//
#include "epoch/worker.h"
#include "epoch/epoch_manager.h"
#include "message/message.h"
#include "transaction/merge.h"
#include "storage/tikv.h"

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
        auto sleep_flag = false;
        while(!EpochManager::IsInitOK()) usleep(1000);
//        printf("State Checker\n");
        while (!EpochManager::IsTimerStop()) {
            sleep_flag = false;
            sleep_flag = sleep_flag | EpochManager::CheckEpochMergeState();
            sleep_flag = sleep_flag | receiveHandler.CheckReceivedStatesAndReply();/// check and send ack
            sleep_flag = sleep_flag | MessageSendHandler::SendEpochEndMessage(ctx);///send epoch end flag
            sleep_flag = sleep_flag | MessageSendHandler::SendBackUpEpochEndMessage(ctx);///send epoch backup end message
            sleep_flag = sleep_flag | MessageSendHandler::SendAbortSet(ctx); ///send abort set
            if(!sleep_flag) usleep(50);
        }
    }

    void WorkerThreadMain(const Context& ctx, uint64_t id) {
        Merger merger;
        merger.Init(ctx, id);
        MessageReceiveHandler receiveHandler;
        receiveHandler.Init(ctx, id);

        auto sleep_flag = false;
        std::unique_ptr<pack_params> pack_param;
        std::unique_ptr<proto::Transaction> txn_ptr;

        while(!EpochManager::IsInitOK()) usleep(1000);

        while(!EpochManager::IsTimerStop()) {
            EpochManager::EpochCacheSafeCheck();
            sleep_flag = sleep_flag | receiveHandler.HandleReceivedMessage();
//            sleep_flag = sleep_flag | merger.EpochMerge();
            sleep_flag = sleep_flag | merger.EpochCommit_RedoLog_TxnMode();
            sleep_flag = sleep_flag | TiKV::sendTransactionToTiKV(EpochManager::GetPushDownEpoch() % ctx.kCacheMaxLength, txn_ptr);
//            if(!sleep_flag) usleep(20);
        }
    }

}

