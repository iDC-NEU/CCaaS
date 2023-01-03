//
// Created by 周慰星 on 11/15/22.
//

#include <queue>
#include <utility>
#include "epoch/merge.h"
#include "epoch/epoch_manager.h"
#include "utils/utilities.h"

namespace Taas {

    void Merger::Init(uint64_t id_, Context ctx_) {
        message_ptr = nullptr;
        txn_ptr = nullptr;
        pack_param = nullptr;
        thread_id = 0, epoch = 0;
        res = false, sleep_flag = false;

        thread_id = id_;
        ctx = std::move(ctx_);

        message_handler.Init(thread_id);

    }

    bool Merger::EpochMerge_RedoLog_ShardingMode() {
        sleep_flag = false;
        while (merge_queue.try_dequeue(txn_ptr) && txn_ptr != nullptr) {
            res = true;
            epoch = txn_ptr->commit_epoch();
            if (!CRDTMerge::ValidateReadSet(ctx, *(txn_ptr))){
                res = false;
            }
            if (!CRDTMerge::MultiMasterCRDTMerge(ctx, *(txn_ptr))) {
                res = false;
            }
            if(res) {
                EpochManager::should_commit_txn_num.IncCount(epoch, thread_id, 1);
                commit_queue.enqueue(std::move(txn_ptr));
                commit_queue.enqueue(nullptr);
            }
            else {
                MessageSendHandler::ReplyTxnStateToClient(ctx, *(txn_ptr), proto::TxnState::Abort);
            }
            EpochManager::merged_txn_num.IncCount(epoch, thread_id, 1);
            sleep_flag = true;
        }
        return sleep_flag;
    }
    ///日志存储为分片模式
    bool Merger::EpochCommit_RedoLog_ShardingMode() {
        sleep_flag = false;
        while (commit_queue.try_dequeue(txn_ptr) && txn_ptr != nullptr) {
            epoch = txn_ptr->commit_epoch();
            if (!CRDTMerge::ValidateWriteSet(ctx, *(txn_ptr))) {
                MessageSendHandler::ReplyTxnStateToClient(ctx, *(txn_ptr), proto::TxnState::Abort);
            }
            else {
                EpochManager::record_commit_txn_num.IncCount(epoch, thread_id, 1);
                CRDTMerge::Commit(ctx, *(txn_ptr));
                CRDTMerge::RedoLog(ctx, *(txn_ptr));
                EpochManager::record_committed_txn_num.IncCount(epoch, thread_id, 1);
                MessageSendHandler::ReplyTxnStateToClient(ctx, *(txn_ptr), proto::TxnState::Commit);
            }
            EpochManager::committed_txn_num.IncCount(epoch, thread_id, 1);
            sleep_flag = true;
        }
        return sleep_flag;
    }





    bool Merger::EpochMerge_RedoLog_TxnMode() {
        sleep_flag = false;
        while (merge_queue.try_dequeue(txn_ptr) && txn_ptr != nullptr) {
            res = true;
            epoch = txn_ptr->commit_epoch();
            if (!CRDTMerge::ValidateReadSet(ctx, *(txn_ptr))){
                res = false;
            }
            if (!CRDTMerge::MultiMasterCRDTMerge(ctx, *(txn_ptr))) {
                res = false;
            }
            EpochManager::merged_txn_num.IncCount(epoch, thread_id, 1);
            sleep_flag = true;
        }
        return sleep_flag;
    }
    ///日志存储为整个事务模式
    bool Merger::EpochCommit_RedoLog_TxnMode() {
        sleep_flag = false;
        while (local_txn_queue.try_dequeue(txn_ptr) && txn_ptr != nullptr) {
            epoch = txn_ptr->commit_epoch();
            if (!CRDTMerge::ValidateWriteSet(ctx, *(txn_ptr))) {
                MessageSendHandler::ReplyTxnStateToClient(ctx, *(txn_ptr), proto::TxnState::Abort);
            }
            else {
                EpochManager::record_commit_txn_num.IncCount(epoch, thread_id, 1);
                CRDTMerge::Commit(ctx, *(txn_ptr));
                CRDTMerge::RedoLog(ctx, *(txn_ptr));
                EpochManager::record_committed_txn_num.IncCount(epoch, thread_id, 1);
                MessageSendHandler::ReplyTxnStateToClient(ctx, *(txn_ptr), proto::TxnState::Commit);
            }
            EpochManager::committed_txn_num.IncCount(epoch, thread_id, 1);
            sleep_flag = true;
        }
        return sleep_flag;
    }

    bool Merger::Run(uint64_t id_, Context ctx_) {
        Init(id_, std::move(ctx_));
        while(!EpochManager::IsTimerStop()) {
            sleep_flag = false;

            sleep_flag = sleep_flag | EpochCommit_RedoLog_TxnMode();

            sleep_flag = sleep_flag | EpochMerge_RedoLog_TxnMode();

//            sleep_flag = sleep_flag | message_handler.HandleReceiveMessage();
//            sleep_flag = sleep_flag | message_handler.HandleLocalMergedTxn();
//            sleep_flag = sleep_flag | message_handler.HandleTxnCachea();

//            sleep_flag = sleep_flag | MessageSendHandler::SendEpochSerializedTxn(thread_id, ctx, send_epoch, pack_param);

            if(!sleep_flag) usleep(200);
        }
        return true;
    }
}