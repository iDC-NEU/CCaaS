//
// Created by 周慰星 on 2022/9/14.
//

#include <queue>
#include <utility>
#include "epoch/merge.h"
#include "epoch/epoch_manager.h"
#include "tools/utilities.h"
#include "storage/tikv.h"

namespace Taas {

/**
 * @brief do local_merge remote_merge and commit
 *
 * @param ctx XML中的配置相关信息
 * @return true
 * @return false
 */

    void StateChecker(Context ctx) {
        Merger merger;
        merger.Init(0, std::move(ctx));

        auto sleep_flag = false;
        std::unique_ptr<pack_params> pack_param;
        std::unique_ptr<proto::Transaction> txn_ptr;
        MessageReceiveHandler receiveHandler;
        MessageSendHandler sendHandler;
        sendHandler.Init(ctx);
        receiveHandler.Init(0, ctx);
        MessageReceiveHandler::StaticInit(ctx);
        uint64_t redo_log_epoch = 1, merge_epoch = 1;
        while (!init_ok.load()) usleep(100);

        while (!EpochManager::IsTimerStop()) {
            sleep_flag = sleep_flag | receiveHandler.StaticClear();///clear receive handler cache
            sleep_flag = sleep_flag | receiveHandler.CheckReceivedStatesAndReply();/// check and send ack

            sleep_flag = sleep_flag | sendHandler.SendEpochEndMessage(ctx);///send epoch end flag
            sleep_flag = sleep_flag | sendHandler.SendBackUpEpochEndMessage(ctx);///send epoch backup end message
            sleep_flag = sleep_flag | sendHandler.SendAbortSet(ctx); ///send abort set
            if(!sleep_flag) usleep(50);
        }
    }

    void WorkerThreadMain(uint64_t id, Context ctx) {
        Merger merger;
        merger.Init(id, std::move(ctx));

        auto sleep_flag = false;
        std::unique_ptr<pack_params> pack_param;
        std::unique_ptr<proto::Transaction> txn_ptr;
        MessageReceiveHandler receiveHandler;
        MessageSendHandler sendHandler;
        sendHandler.Init(ctx);
        receiveHandler.Init(id, ctx);
        uint64_t redo_log_epoch = 1, merge_epoch = 1;
        while (!init_ok.load()) usleep(100);

        while(!EpochManager::IsTimerStop()) {
            EpochManager::EpochCacheSafeCheck();
            sleep_flag = sleep_flag | receiveHandler.HandleReceivedMessage();
            sleep_flag = sleep_flag | merger.EpochMerge();
            sleep_flag = sleep_flag | merger.EpochCommit_RedoLog_TxnMode();
            sleep_flag = sleep_flag | sendTransactionToTiKV(EpochManager::GetPushDownEpoch() % ctx.kCacheMaxLength, txn_ptr);
            if(!sleep_flag) usleep(50);
        }

    }

}

