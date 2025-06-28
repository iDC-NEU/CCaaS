//
// Created by 周慰星 on 11/15/22.
//

#include <utility>
#include "transaction/merge.h"
#include "epoch/epoch_manager.h"
#include "tools/utilities.h"
#include "storage/redo_loger.h"
#include "transaction/transaction_cache.h"

namespace Taas {


    void Merger::MergeInit(const uint64_t &id) {

        txn_ptr.reset();
        message_ptr = nullptr;
        shard_num = TaasContext::kTxnNodeNum;
        local_server_id = TaasContext::txn_node_ip_index;
        ThreadCountersInit(ctx);
    }


    void Merger::ReadValidateQueueEnqueue(uint64_t &epoch_, const std::shared_ptr<proto::Transaction>& txn_ptr_) {
        auto epoch_mod_temp = epoch_ % TaasContext::kCacheMaxLength;
        epoch_should_read_validate_txn_num_local->IncCount(epoch_mod_temp, txn_ptr_->txn_server_id(), 1);
        TransactionCache::epoch_read_validate_queue[epoch_mod_temp]->enqueue(txn_ptr_);
        TransactionCache::epoch_read_validate_queue[epoch_mod_temp]->enqueue(nullptr);
    }
    void Merger::MergeQueueEnqueue(uint64_t &epoch_, const std::shared_ptr<proto::Transaction>& txn_ptr_) {
        auto epoch_mod_temp = epoch_ % TaasContext::kCacheMaxLength;
        epoch_should_merge_txn_num_local->IncCount(epoch_mod_temp, txn_ptr->txn_server_id(), 1);
        TransactionCache::epoch_merge_queue[epoch_mod_temp]->enqueue(txn_ptr_);
        TransactionCache::epoch_merge_queue[epoch_mod_temp]->enqueue(nullptr);
    }
    void Merger::CommitQueueEnqueue(uint64_t& epoch_, const std::shared_ptr<proto::Transaction>& txn_ptr_) {
        auto epoch_mod_temp = epoch_ % TaasContext::kCacheMaxLength;
        epoch_should_commit_txn_num_local->IncCount(epoch_mod_temp, txn_ptr_->txn_server_id(), 1);
        TransactionCache::epoch_commit_queue[epoch_mod_temp]->enqueue(txn_ptr_);
        TransactionCache::epoch_commit_queue[epoch_mod_temp]->enqueue(nullptr);
    }
    void Merger::ResultReturnQueueEnqueue(uint64_t& epoch_, const std::shared_ptr<proto::Transaction>& txn_ptr_) {
        auto epoch_mod_temp = epoch_ % TaasContext::kCacheMaxLength;
        epoch_result_return_txn_num_local->IncCount(epoch_mod_temp, txn_ptr_->txn_server_id(), 1);
        TransactionCache::epoch_result_return_queue[epoch_mod_temp]->enqueue(txn_ptr_);
        TransactionCache::epoch_result_return_queue[epoch_mod_temp]->enqueue(nullptr);
    }

    bool Merger::MergeQueueTryDequeue(uint64_t &epoch_, const std::shared_ptr<proto::Transaction>& txn_ptr_) {
        ///not use for now
        return false;
    }
    bool Merger::CommitQueueTryDequeue(uint64_t& epoch_, std::shared_ptr<proto::Transaction> txn_ptr_) {
        auto epoch_mod_temp = epoch_ % TaasContext::kCacheMaxLength;
        return TransactionCache::epoch_commit_queue[epoch_mod_temp]->try_dequeue(txn_ptr_);
    }




    void Merger::Send() {
    }

    void Merger::ReadValidate() {
        message_epoch = txn_ptr->commit_epoch();
        message_epoch_mod = message_epoch % TaasContext::kCacheMaxLength;
        message_server_id = txn_ptr->txn_server_id();
        shard_id = txn_ptr->shard_id();
        shard_server_id = txn_ptr->shard_server_id();
        auto time1 = now_to_us();
        if (CRDTMerge::ValidateReadSet(txn_ptr)) {
            /// already enqueue merge_queue, commit_queue, redo_log_queue, result_return_queue
        }
        else {
            total_read_version_check_failed_txn_num_local.fetch_add(1);
            csn_temp = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->txn_server_id());
            TransactionCache::epoch_abort_txn_set[message_epoch_mod]->insert(csn_temp, csn_temp);
        }
        epoch_read_validated_txn_num_local->IncCount(message_epoch, message_server_id, 1);
//        LOG(INFO) << "Validate Time Cost " << now_to_us() - time1 << " us";
        total_single_validate_time += now_to_us() - time1;
        total_single_validate_num ++;
    }

    void Merger::Merge() {
        auto time1 = now_to_us();
        epoch = txn_ptr->commit_epoch();
        CRDTMerge::MultiMasterCRDTMerge(txn_ptr);
        total_merge_txn_num_local.fetch_add(1);
        total_merge_latency_local.fetch_add(now_to_us() - time1);
        epoch_merged_txn_num_local->IncCount(epoch, txn_server_id, 1);
//        LOG(INFO) << "Merge Time Cost " << now_to_us() - time1 << " us";
        total_single_merge_time += now_to_us() - time1;
        total_single_merge_num ++;
    }

    void Merger::Commit() {
        auto time1 = now_to_us();
        if (CRDTMerge::ValidateWriteSet(txn_ptr)) {
            CRDTMerge::Commit(txn_ptr);
        }
        epoch_committed_txn_num_local->IncCount(epoch, txn_ptr->txn_server_id(), 1);
//        LOG(INFO) << "Commit Time Cost " << now_to_us() - time1 << " us";
        total_single_commit_time += now_to_us() - time1;
        total_single_commit_num ++;
    }

    void Merger::RedoLog() {
        auto time1 = now_to_us();
        if (!CRDTMerge::ValidateWriteSet(txn_ptr)) {
            total_failed_txn_num_local.fetch_add(1);
//            EpochMessageSendHandler::SendTxnCommitResultToClient(txn_ptr, proto::TxnState::Abort);
        } else {
            RedoLoger::RedoLog(thread_id, txn_ptr);
//            EpochMessageSendHandler::SendTxnCommitResultToClient(txn_ptr, proto::TxnState::Commit);
            success_commit_txn_num_local.fetch_add(1);
            success_commit_latency_local.fetch_add(now_to_us() - time1);
        }
        total_commit_txn_num_local.fetch_add(1);
        total_commit_latency_local.fetch_add(now_to_us() - time1);
//        LOG(INFO) << "******* Merge RedoLog Epoch : " << epoch << "txn_server_id" << txn_ptr->txn_server_id() << "********\n";
        epoch_record_committed_txn_num_local->IncCount(epoch, txn_ptr->txn_server_id(), 1);
//        LOG(INFO) << "RedoLog Time Cost " << now_to_us() - time1 << " us";
        total_single_log_time += now_to_us() - time1;
        total_single_log_num ++;
    }

    void Merger::ResultReturn() {
        auto time1 = now_to_us();
        if (!CRDTMerge::ValidateWriteSet(txn_ptr)) {
            EpochMessageSendHandler::SendTxnCommitResultToClient(txn_ptr, proto::TxnState::Abort);
        } else {
            EpochMessageSendHandler::SendTxnCommitResultToClient(txn_ptr, proto::TxnState::Commit);
        }
        epoch_result_returned_txn_num_local->IncCount(epoch, txn_ptr->txn_server_id(), 1);
        total_single_result_time += now_to_us() - time1;
        total_single_result_num ++;
        total_single_time += now_to_us() - txn_ptr->csn();
        total_single_num ++;
        if(total_single_num > 0 &&  total_single_num % TaasContext::print_mode_size == 0) {
            LOG(INFO) << " Validate Time Cost : " << total_single_validate_time  << " Validate Time count : " << total_single_validate_num << " Validate avg : " << total_single_validate_time/total_single_validate_num
                      << " Merge Time Cost : " << total_single_merge_time << " Merge Time count : " << total_single_merge_num << " Validate avg : " << total_single_merge_time/total_single_merge_num
                      << " Commit Time Cost : " << total_single_commit_time << " Commit Time count : " << total_single_commit_num << " Validate avg : " << total_single_commit_time/total_single_commit_num
                      << " RedoLog Time Cost : " << total_single_log_time << " RedoLog Time count : " << total_single_log_num << " Validate avg : " << total_single_log_time/total_single_log_num
                      << " ResultReturn Time Cost : " << total_single_result_time << " ResultReturn Time count : " << total_single_result_num << " Validate avg : " << total_single_result_time/total_single_result_num
                      << " Total Time Cost : " << total_single_time << " Total Time count : " << total_single_num << "Validate avg : " << total_single_time/total_single_num
                      << " end";
        }
//        LOG(INFO) << "ResultReturn Time Cost " << now_to_us() - time1 << " us";
//        LOG(INFO) << "Total Cost " << now_to_us() - txn_ptr->csn() << " us";
    }

    void Merger::EpochMerge() {
        epoch = EpochManager::GetLogicalEpoch();
        while (!EpochManager::IsTimerStop()) {
            sleep_flag = true;
            epoch = EpochManager::GetLogicalEpoch();
            epoch_mod = epoch % TaasContext::kCacheMaxLength;

            while(TransactionCache::epoch_read_validate_queue[epoch_mod]->try_dequeue(txn_ptr)) { /// only local txn do this procedure
                if (txn_ptr != nullptr && txn_ptr->txn_type() != proto::TxnType::NullMark) {
                    ReadValidate();
                    txn_ptr.reset();
                    sleep_flag = false;
                }
            }

            if(!EpochManager::IsEpochMergeComplete(epoch)) {
                while (TransactionCache::epoch_merge_queue[epoch_mod]->try_dequeue(txn_ptr)) {
                    if (txn_ptr != nullptr && txn_ptr->txn_type() != proto::TxnType::NullMark) {
                        Merge();
                        txn_ptr.reset();
                        sleep_flag = false;
                    }
                }
            }

            if(EpochManager::IsAbortSetMergeComplete(epoch) && !EpochManager::IsCommitComplete(epoch)) {
                while (!EpochManager::IsCommitComplete(epoch) &&
                       TransactionCache::epoch_commit_queue[epoch_mod]->try_dequeue(txn_ptr)) {
                    if (txn_ptr != nullptr && txn_ptr->txn_type() != proto::TxnType::NullMark) {
                        Commit();
                        txn_ptr.reset();
                        sleep_flag = false;
                    }
                }
            }

            if(EpochManager::IsAbortSetMergeComplete(epoch) && !EpochManager::IsRecordCommitted(epoch)) {
//                LOG(INFO) << "******* Merge RedoLog 1 : " << epoch << "********\n";
                while (!EpochManager::IsRecordCommitted(epoch) && TransactionCache::epoch_redo_log_queue[epoch_mod]->try_dequeue(txn_ptr)) {
                    if (txn_ptr != nullptr && txn_ptr->txn_type() != proto::TxnType::NullMark) { /// only local txn do redo log
//                        LOG(INFO) << "******* Merge RedoLog 2 : " << epoch << "txn_server_id" << txn_ptr->txn_server_id() << "********\n";
                        RedoLog();
                        txn_ptr.reset();
                        sleep_flag = false;
                    }
                }
            }

            if(sleep_flag)
                usleep(merge_sleep_time);
        }
    }
}