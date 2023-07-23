//
// Created by zwx on 23-7-3.
//

#include "epoch/epoch_manager.h"
#include "epoch/epoch_manager_sharding.h"
#include "message/epoch_message_receive_handler.h"
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
               (EpochMessageReceiveHandler::CheckEpochShardingSendComplete(ctx, i) &&
                       EpochMessageReceiveHandler::CheckEpochShardingReceiveComplete(ctx, i) &&
                       EpochMessageReceiveHandler::CheckEpochBackUpComplete(ctx, i))
              ) &&
              Merger::CheckEpochMergeComplete(ctx, i)
                ) {
            EpochManager::SetShardingMergeComplete(i, true);
            merge_epoch.fetch_add(1);
            LOG(INFO) << "**** Finished Epoch Merge Epoch : " << i << "****\n";
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
            (ctx.kTxnNodeNum == 1 || EpochMessageReceiveHandler::CheckEpochAbortSetMergeComplete(ctx, i))) {

            EpochManager::SetAbortSetMergeComplete(i, true);
            abort_set_epoch.fetch_add(1);
            LOG(INFO) << "******** Finished Abort Set Merge Epoch : " << i << "********\n";
            i ++;
            return true;
        }
        return false;
    }

    static uint64_t last_total_commit_txn_num = 0;
    bool ShardingEpochManager::CheckEpochCommitState(const Context& ctx) {
        if(commit_epoch.load() >= abort_set_epoch.load()) return false;
        auto i = commit_epoch.load();
        if( i < abort_set_epoch.load() && EpochManager::IsShardingMergeComplete(i) &&
            EpochManager::IsAbortSetMergeComplete(i) &&
            Merger::CheckEpochCommitComplete(ctx, i)
                ) {
            EpochManager::SetCommitComplete(i, true);
            auto epoch_commit_success_txn_num = Merger::epoch_record_committed_txn_num.GetCount(i);
            total_commit_txn_num += epoch_commit_success_txn_num;///success
            LOG(INFO) << PrintfToString("************ 完成一个Epoch的合并 Epoch: %lu, EpochSuccessCommitTxnNum: %lu, EpochCommitTxnNum: %lu ************\n",
                                        i, epoch_commit_success_txn_num, EpochMessageSendHandler::TotalTxnNum.load() - last_total_commit_txn_num);
            if(i % ctx.print_mode_size == 0) {
                LOG(INFO) << PrintfToString("Epoch: %lu ClearEpoch: %lu, SuccessTxnNumber %lu, ToTalSuccessLatency %lu, SuccessAvgLatency %lf, TotalCommitTxnNum %lu, TotalCommitlatency %lu, TotalCommitAvglatency %lf ************\n",
                                            i, clear_epoch.load(),
                                            EpochMessageSendHandler::TotalSuccessTxnNUm.load(), EpochMessageSendHandler::TotalSuccessLatency.load(),
                                            (((double)EpochMessageSendHandler::TotalSuccessLatency.load()) / ((double)EpochMessageSendHandler::TotalSuccessTxnNUm.load())),
                                            EpochMessageSendHandler::TotalTxnNum.load(),///receive from client
                                            EpochMessageSendHandler::TotalLatency.load(),
                                            (((double)EpochMessageSendHandler::TotalLatency.load()) / ((double)EpochMessageSendHandler::TotalTxnNum.load())));
            }
            last_total_commit_txn_num = EpochMessageSendHandler::TotalTxnNum.load();
            i ++;
            commit_epoch.fetch_add(1);
            EpochManager::AddLogicalEpoch();
            return true;
        }
        return false;
    }


    void ShardingEpochManager::EpochLogicalTimerManagerThreadMain(const Context& ctx) {
        while(!EpochManager::IsInitOK()) usleep(sleep_time);
        uint64_t epoch = 1;
        OUTPUTLOG(ctx, "===== Start Epoch的合并 ===== ", epoch);
        while(!EpochManager::IsTimerStop()){

            while(!(epoch < EpochManager::GetPhysicalEpoch() &&
                    (ctx.kTxnNodeNum == 1 ||
                     (EpochMessageReceiveHandler::CheckEpochShardingSendComplete(ctx, epoch) &&
                      EpochMessageReceiveHandler::CheckEpochShardingReceiveComplete(ctx, epoch) &&
                      EpochMessageReceiveHandler::CheckEpochBackUpComplete(ctx, epoch))
                    ) && Merger::CheckEpochMergeComplete(ctx, epoch))) {
                usleep(logical_sleep_timme);
            }

            EpochManager::SetShardingMergeComplete(epoch, true);
            merge_epoch.fetch_add(1);
            LOG(INFO) << "**** Finished Epoch Merge Epoch : " << epoch << "****\n";

            while(! (epoch < merge_epoch.load()  && EpochManager::IsShardingMergeComplete(epoch) &&
                     (ctx.kTxnNodeNum == 1 || EpochMessageReceiveHandler::CheckEpochAbortSetMergeComplete(ctx, epoch)))){
                usleep(logical_sleep_timme);
            }
            EpochManager::SetAbortSetMergeComplete(epoch, true);
            abort_set_epoch.fetch_add(1);
            LOG(INFO) << "******* Finished Abort Set Merge Epoch : " << epoch << "********\n";

            while(!( epoch < abort_set_epoch.load() && EpochManager::IsShardingMergeComplete(epoch) &&
                     EpochManager::IsAbortSetMergeComplete(epoch) &&
                     Merger::CheckEpochCommitComplete(ctx, epoch))) {
                usleep(logical_sleep_timme);
            }
            EpochManager::SetCommitComplete(epoch, true);
            last_total_commit_txn_num = EpochMessageSendHandler::TotalTxnNum.load();
            epoch ++;
            commit_epoch.fetch_add(1);
            EpochManager::AddLogicalEpoch();

            auto epoch_commit_success_txn_num = Merger::epoch_record_committed_txn_num.GetCount(epoch);
            total_commit_txn_num += epoch_commit_success_txn_num;///success
            LOG(INFO) << PrintfToString("************ 完成一个Epoch的合并 Epoch: %lu, EpochSuccessCommitTxnNum: %lu, EpochCommitTxnNum: %lu ************\n",
                                        epoch, epoch_commit_success_txn_num, EpochMessageSendHandler::TotalTxnNum.load() - last_total_commit_txn_num);
            if(epoch % ctx.print_mode_size == 0) {
                LOG(INFO) << PrintfToString("Epoch: %lu ClearEpoch: %lu, SuccessTxnNumber %lu, ToTalSuccessLatency %lu, SuccessAvgLatency %lf, TotalCommitTxnNum %lu, TotalCommitlatency %lu, TotalCommitAvglatency %lf ************\n",
                                            epoch, clear_epoch.load(),
                                            EpochMessageSendHandler::TotalSuccessTxnNUm.load(), EpochMessageSendHandler::TotalSuccessLatency.load(),
                                            (((double)EpochMessageSendHandler::TotalSuccessLatency.load()) / ((double)EpochMessageSendHandler::TotalSuccessTxnNUm.load())),
                                            EpochMessageSendHandler::TotalTxnNum.load(),///receive from client
                                            EpochMessageSendHandler::TotalLatency.load(),
                                            (((double)EpochMessageSendHandler::TotalLatency.load()) / ((double)EpochMessageSendHandler::TotalTxnNum.load())));
            }
        }
        printf("total commit txn num: %lu\n", total_commit_txn_num);
    }
}

