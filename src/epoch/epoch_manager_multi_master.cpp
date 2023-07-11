//
// Created by zwx on 23-7-3.
//
#include "epoch/epoch_manager.h"
#include "epoch/epoch_manager_multi_master.h"
#include "message/handler_receive.h"
#include "storage/redo_loger.h"
#include "transaction/merge.h"

#include "string"

namespace Taas {

    bool MultiMasterEpochManager::CheckEpochMergeState(const Context& ctx) {
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

    bool MultiMasterEpochManager::CheckEpochAbortMergeState(const Context& ctx) {
        auto i = abort_set_epoch.load();
        if(i >= merge_epoch.load() && commit_epoch.load() >= abort_set_epoch.load()) return false;
        if(EpochManager::IsAbortSetMergeComplete(i)) return true;
        if( i < merge_epoch.load()  && EpochManager::IsShardingMergeComplete(i)) {
            /// in multi master mode, there is no need to send and merge sharding abort set
            EpochManager::SetAbortSetMergeComplete(i, true);
            abort_set_epoch.fetch_add(1);
            return true;
        }
        return false;
    }

    bool MultiMasterEpochManager::CheckEpochCommitState(const Context& ctx) {
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
                if(MessageSendHandler::TotalTxnNum.load() != 0);
                printf("Taas Totallatency %lu TotalNum %lu avg %f\n", MessageSendHandler::TotalLatency.load(),
                       MessageSendHandler::TotalTxnNum.load(), (((double)MessageSendHandler::TotalLatency.load()) / ((double)MessageSendHandler::TotalTxnNum.load())));
            }
            i ++;
            commit_epoch.fetch_add(1);
            EpochManager::AddLogicalEpoch();
            return true;
        }
        return false;
    }
}

