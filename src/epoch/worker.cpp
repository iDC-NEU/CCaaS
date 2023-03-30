//
// Created by 周慰星 on 23-3-30.
//

#include "epoch/worker.h"
//
// Created by 周慰星 on 2022/9/14.
//

#include <queue>
#include <utility>

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

    void StateChecker(Context ctx) {
        Merger::StaticInit(ctx);
        InitMessage(ctx);
        MessageReceiveHandler::StaticInit(ctx);


        init_ok_num.fetch_add(1);


        MessageSendHandler sendHandler;
        sendHandler.Init(ctx);
        MessageReceiveHandler receiveHandler;
        receiveHandler.Init(0, ctx);

        auto sleep_flag = false;

        while(!EpochManager::IsInitOK()) usleep(1000);
        printf("State Checker\n");
        while (!EpochManager::IsTimerStop()) {
            sleep_flag = false;
//            printf("State Checker 48\n");
            sleep_flag = sleep_flag | receiveHandler.StaticClear();///clear receive handler cache
//            printf("State Checker 50\n");
            sleep_flag = sleep_flag | receiveHandler.CheckReceivedStatesAndReply();/// check and send ack
//            printf("State Checker 52\n");

            sleep_flag = sleep_flag | sendHandler.SendEpochEndMessage(ctx);///send epoch end flag
//            printf("State Checker 55\n");
            sleep_flag = sleep_flag | sendHandler.SendBackUpEpochEndMessage(ctx);///send epoch backup end message
//            printf("State Checker 57\n");
            sleep_flag = sleep_flag | sendHandler.SendAbortSet(ctx); ///send abort set
//            printf("State Checker 59\n");
            if(!sleep_flag) usleep(50);
        }
    }

    void WorkerThreadMain(uint64_t id, Context ctx) {
        Merger merger;
        merger.Init(id, std::move(ctx));
        MessageSendHandler sendHandler;
        sendHandler.Init(ctx);
        MessageReceiveHandler receiveHandler;
        receiveHandler.Init(id, ctx);

        auto sleep_flag = false;
        std::unique_ptr<pack_params> pack_param;
        std::unique_ptr<proto::Transaction> txn_ptr;

        while(!EpochManager::IsInitOK()) usleep(1000);

        while(!EpochManager::IsTimerStop()) {
            EpochManager::EpochCacheSafeCheck();
            sleep_flag = sleep_flag | receiveHandler.HandleReceivedMessage();
            sleep_flag = sleep_flag | merger.EpochMerge();
            sleep_flag = sleep_flag | merger.EpochCommit_RedoLog_TxnMode();
            sleep_flag = sleep_flag | TiKV::sendTransactionToTiKV(EpochManager::GetPushDownEpoch() % ctx.kCacheMaxLength, txn_ptr);
            if(!sleep_flag) usleep(50);
        }

    }

}

