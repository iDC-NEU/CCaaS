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
    void Run(uint64_t id, Context ctx) {
        Merger merger;
        merger.Init(id, std::move(ctx));

        auto sleep_flag = false;
        std::unique_ptr<pack_params> pack_param;

        MessageReceiveHandler receiveHandler;
        MessageSendHandler sendHandler;
        sendHandler.Init(id, ctx);
        receiveHandler.Init(id, ctx);
        while (!init_ok.load()) usleep(100);
        while(!EpochManager::IsTimerStop()) {
            EpochManager::EpochCacheSafeCheck();

            sleep_flag = sleep_flag | sendHandler.HandlerSendTask(id, ctx);

            sleep_flag = sleep_flag | receiveHandler.HandleReceivedMessage();
            sleep_flag = sleep_flag | receiveHandler.HandleTxnCache();

            sleep_flag = sleep_flag | merger.EpochMerge();
            sleep_flag = sleep_flag | merger.EpochCommit_RedoLog_TxnMode();


            if(id == 0) {
                sleep_flag = sleep_flag | receiveHandler.CheckReceivedStatesAndReply();
                sleep_flag = sleep_flag | receiveHandler.ClearStaticCounters();

                sleep_flag = sleep_flag | sendHandler.SendEpochEndMessage(id, ctx);
                sleep_flag = sleep_flag | sendHandler.SendBackUpEpochEndMessage(id, ctx);
                sleep_flag = sleep_flag | sendHandler.SendAbortSet(id, ctx); ///send abort set
//            sleep_flag = sleep_flag | sendHandler.SendInsertSet(id, ctx, isnert_set_send_epoch);///send abort set
            }

            sleep_flag = sleep_flag | sendTransactionToTiKV();

            if(!sleep_flag) usleep(50);
        }

    }

}

