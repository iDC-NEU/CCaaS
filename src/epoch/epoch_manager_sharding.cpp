//
// Created by zwx on 23-7-3.
//

#include "epoch/epoch_manager.h"
#include "epoch/epoch_manager_sharding.h"
#include "message/handler_receive.h"
#include "storage/redo_loger.h"
#include "transaction/merge.h"

#include "string"

namespace Taas {

    bool ShardingEpochManager::CheckEpochMergeState(const Context& ctx) {
        auto res = false;
        while (EpochManager::IsShardingMergeComplete(merge_epoch.load()) &&
               merge_epoch.load() < EpochManager::GetPhysicalEpoch()) {
            merge_epoch.fetch_add(1);
        }
        auto i = merge_epoch.load();
        while(i < EpochManager::GetPhysicalEpoch() &&
              (ctx.kTxnNodeNum == 1 ||
               (MessageReceiveHandler::CheckEpochShardingSendComplete(ctx, i) &&
                MessageReceiveHandler::CheckEpochShardingReceiveComplete(ctx, i) &&
                MessageReceiveHandler::CheckEpochBackUpComplete(ctx, i))
              ) &&
              Merger::CheckEpochMergeComplete(ctx, i)
                ) {
            EpochManager::SetShardingMergeComplete(i, true);
            merge_epoch.fetch_add(1);
            i ++;
            res = true;
        }
        return res;
    }

    bool ShardingEpochManager::CheckEpochAbortMergeState(const Context& ctx) {
        auto i = abort_set_epoch.load();
        if(i >= merge_epoch.load() && commit_epoch.load() >= abort_set_epoch.load()) return false;
        if(EpochManager::IsAbortSetMergeComplete(i)) return true;
        if( i < merge_epoch.load()  && EpochManager::IsShardingMergeComplete(i) &&
            (ctx.kTxnNodeNum == 1 || MessageReceiveHandler::CheckEpochAbortSetMergeComplete(ctx, i))) {

            EpochManager::SetAbortSetMergeComplete(i, true);
            //            Merger::GenerateCommitTask(ctx, i);
            abort_set_epoch.fetch_add(1);
            i ++;
            return true;
        }
        return false;
    }

    bool ShardingEpochManager::CheckEpochCommitState(const Context& ctx) {
        if(commit_epoch.load() >= abort_set_epoch.load()) return false;
        auto i = commit_epoch.load();
        if( i < abort_set_epoch.load() && EpochManager::IsShardingMergeComplete(i) &&
            EpochManager::IsAbortSetMergeComplete(i) &&
            Merger::CheckEpochCommitComplete(ctx, i)
                ) {
            EpochManager::SetCommitComplete(i, true);
            //            RedoLoger::GeneratePushDownTask(ctx, i);
            total_commit_txn_num += Merger::epoch_record_committed_txn_num.GetCount(i);

            if(i % ctx.print_mode_size == 0) {
                printf("*************       完成一个Epoch的合并     Epoch: %8lu ClearEpoch: %8lu *************\n", i, clear_epoch.load());
                printf("commit txn total number %lu\n", total_commit_txn_num);
            }
            i ++;
            commit_epoch.fetch_add(1);
            EpochManager::AddLogicalEpoch();
            return true;
        }
        return false;
    }
}

