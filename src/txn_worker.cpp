//
// Created by 周慰星 on 2022/9/14.
//

#include <queue>
#include <utility>
#include "epoch/merge.h"
#include "epoch/epoch_manager.h"
#include "utils/utilities.h"

namespace Taas {

/**
 * @brief do local_merge remote_merge and commit
 *
 * @param ctx XML中的配置相关信息
 * @return true
 * @return false
 */
    void MergeWorkerThreadMain(uint64_t id, Context ctx) {
        Merger merger;
        merger.Run(id, ctx);

        merger.Init(id, std::move(ctx));
        auto sleep_flag = false;
        while(!EpochManager::IsTimerStop()) {
            sleep_flag = false;

            sleep_flag = sleep_flag | merger.EpochMerge();

            sleep_flag = sleep_flag | merger.EpochCommit();

            sleep_flag = sleep_flag | merger.LocalMerge();

            if(!sleep_flag) usleep(200);
        }
    }
}

