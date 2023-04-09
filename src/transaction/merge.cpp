//
// Created by 周慰星 on 11/15/22.
//

#include <utility>
#include "transaction/merge.h"
#include "epoch/epoch_manager.h"
#include "tools/utilities.h"
#include "storage/redo_loger.h"

namespace Taas {

    AtomicCounters_Cache
            Merger::epoch_should_merge_txn_num(10, 2),
            Merger::epoch_merged_txn_num(10, 2),
            Merger::epoch_should_commit_txn_num(10, 2),
            Merger::epoch_committed_txn_num(10, 2),
            Merger::epoch_record_commit_txn_num(10, 2),
            Merger::epoch_record_committed_txn_num(10, 2);

    std::vector<std::unique_ptr<concurrent_crdt_unordered_map<std::string, std::string, std::string>>>
            Merger::epoch_merge_map, ///epoch merge   row_header
            Merger::local_epoch_abort_txn_set,
            Merger::epoch_abort_txn_set; /// for epoch final check

    std::vector<std::unique_ptr<concurrent_unordered_map<std::string, std::string>>>
            Merger::epoch_insert_set;

    concurrent_unordered_map<std::string, std::string>
            Merger::read_version_map, ///read validate for higher isolation
            Merger::insert_set,   ///插入集合，用于判断插入是否可以执行成功 check key exits?
            Merger::abort_txn_set; /// 所有abort的事务，不区分epoch

    std::unique_ptr<moodycamel::BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>> Merger::task_queue;
//    std::unique_ptr<BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>> Merger::merge_queue;///存放要进行merge的事务，分片`
    std::unique_ptr<BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>> Merger::commit_queue;
    std::vector<std::unique_ptr<BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>>
            Merger::epoch_merge_queue,///存放要进行merge的事务，分片`
            Merger::epoch_local_txn_queue,///存放epoch由client发送过来的事务，存放每个epoch要进行写日志的事务，整个事务写日志
            Merger::epoch_commit_queue;///存放每个epoch要进行写日志的事务，分片写日志

    std::vector<std::unique_ptr<std::atomic<bool>>>
            Merger::epoch_merge_complete,
            Merger::epoch_commit_complete;

    void Merger::StaticInit(const Context &ctx) {
        auto max_length = ctx.kCacheMaxLength;
        auto pack_num = ctx.kIndexNum;
        ///epoch merge state
        epoch_merge_map.resize(max_length);
        local_epoch_abort_txn_set.resize(max_length);
        epoch_abort_txn_set.resize(max_length);
        epoch_insert_set.resize(max_length);

//        ///transaction concurrent queue
        task_queue = std::make_unique<BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>();
//        merge_queue = std::make_unique<BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>();
        commit_queue = std::make_unique<BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>();

        epoch_merge_queue.resize(max_length);
        epoch_local_txn_queue.resize(max_length);
        epoch_commit_queue.resize(max_length);

        epoch_merge_complete.resize(max_length);
        epoch_commit_complete.resize(max_length);

        epoch_should_merge_txn_num.Init(max_length, pack_num);
        epoch_merged_txn_num.Init(max_length, pack_num);
        epoch_should_commit_txn_num.Init(max_length, pack_num);
        epoch_committed_txn_num.Init(max_length, pack_num);
        epoch_record_commit_txn_num.Init(max_length, pack_num);
        epoch_record_committed_txn_num.Init(max_length, pack_num);

        for(int i = 0; i < static_cast<int>(max_length); i ++) {
            epoch_merge_complete[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_commit_complete[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_merge_map[i] = std::make_unique<concurrent_crdt_unordered_map<std::string, std::string, std::string>>();
            local_epoch_abort_txn_set[i] = std::make_unique<concurrent_crdt_unordered_map<std::string, std::string, std::string>>();
            epoch_abort_txn_set[i] = std::make_unique<concurrent_crdt_unordered_map<std::string, std::string, std::string>>();
            epoch_insert_set[i] = std::make_unique<concurrent_unordered_map<std::string, std::string>>();

            epoch_merge_queue[i] = std::make_unique<BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>();
            epoch_local_txn_queue[i] = std::make_unique<BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>();
            epoch_commit_queue[i] = std::make_unique<BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>();
        }
        RedoLoger::StaticInit(ctx);
    }

    void Merger::MergeQueueEnqueue(const Context& ctx, uint64_t& epoch, std::unique_ptr<proto::Transaction>&& txn_ptr) {
        auto epoch_mod = epoch % ctx.kCacheMaxLength;
        epoch_merge_queue[epoch_mod]->enqueue(std::move(txn_ptr));
        epoch_merge_queue[epoch_mod]->enqueue(nullptr);
    }
    bool Merger::MergeQueueTryDequeue(const Context& ctx, uint64_t& epoch, std::unique_ptr<proto::Transaction>& txn_ptr) {
        auto epoch_mod = epoch % ctx.kCacheMaxLength;
        return epoch_merge_queue[epoch_mod]->try_dequeue(txn_ptr);
    }

    void Merger::LocalTxnCommitQueueEnqueue(const Context& ctx, uint64_t& epoch, std::unique_ptr<proto::Transaction>&& txn_ptr) {
        auto epoch_mod = epoch % ctx.kCacheMaxLength;
        epoch_local_txn_queue[epoch_mod]->enqueue(std::move(txn_ptr));
        epoch_local_txn_queue[epoch_mod]->enqueue(nullptr);
    }
    bool Merger::LocalTxnCommitQueueTryDequeue(const Context& ctx, uint64_t& epoch, std::unique_ptr<proto::Transaction>& txn_ptr) {
        auto epoch_mod = epoch % ctx.kCacheMaxLength;
        return epoch_local_txn_queue[epoch_mod]->try_dequeue(txn_ptr);
    }
    void Merger::CommitQueueEnqueue(const Context& ctx, uint64_t& epoch, std::unique_ptr<proto::Transaction>&& txn_ptr) {
        auto epoch_mod = epoch % ctx.kCacheMaxLength;
        epoch_commit_queue[epoch_mod]->enqueue(std::move(txn_ptr));
        epoch_commit_queue[epoch_mod]->enqueue(nullptr);
    }
    bool Merger::CommitQueueTryDequeue(const Context& ctx, uint64_t& epoch, std::unique_ptr<proto::Transaction>& txn_ptr) {
        auto epoch_mod = epoch % ctx.kCacheMaxLength;
        return epoch_commit_queue[epoch_mod]->try_dequeue(txn_ptr);
    }


    void Merger::ClearMergerEpochState(const Context& ctx, uint64_t& epoch) {
        auto epoch_mod = epoch % ctx.kCacheMaxLength;
        epoch_merge_complete[epoch_mod]->store(false);
        epoch_commit_complete[epoch_mod]->store(false);
        epoch_merge_map[epoch_mod]->clear();
        epoch_insert_set[epoch_mod]->clear();
        epoch_abort_txn_set[epoch_mod]->clear();
        local_epoch_abort_txn_set[epoch_mod]->clear();
        epoch_should_merge_txn_num.Clear(epoch_mod), epoch_merged_txn_num.Clear(epoch_mod);
        epoch_should_commit_txn_num.Clear(epoch_mod), epoch_committed_txn_num.Clear(epoch_mod);
        epoch_record_commit_txn_num.Clear(epoch_mod), epoch_record_committed_txn_num.Clear(epoch_mod);
    }

    void Merger::Init(const Context& ctx_, uint64_t id_) {
        txn_ptr = nullptr;
        thread_id = id_;
        ctx = ctx_;
        message_handler.Init(ctx, thread_id);
    }

    bool Merger::EpochMerge(const Context& ctx, uint64_t& epoch, std::unique_ptr<proto::Transaction>&& txn_ptr) {
        auto epoch_mod = epoch % ctx.kCacheMaxLength;
        auto txn_server_id = txn_ptr->server_id();
        auto res = true;
        if (!CRDTMerge::ValidateReadSet(ctx, *(txn_ptr))) {
            res = false;
        }
        if (!CRDTMerge::MultiMasterCRDTMerge(ctx, *(txn_ptr))) {
            res = false;
        }
        if (res) {
            epoch_should_commit_txn_num.IncCount(epoch, txn_ptr->server_id(), 1);
            epoch_commit_queue[epoch_mod]->enqueue(std::move(txn_ptr));
            epoch_commit_queue[epoch_mod]->enqueue(nullptr);
        }
        epoch_merged_txn_num.IncCount(epoch, txn_server_id, 1);
        CheckEpochMergeComplete(ctx, epoch);
        return res;
    }

    bool Merger::GenerateCommitTask(const Context &ctx_, uint64_t &epoch) {
        auto txn_ptr = std::make_unique<proto::Transaction>();
        txn_ptr->set_commit_epoch(epoch);
        task_queue->enqueue(std::move(txn_ptr));
        task_queue->enqueue(nullptr);
        return true;
    }

    void Merger::EpochCommit_RedoLog_TxnMode_Commit_Queue() {
        while(!EpochManager::IsTimerStop()) {
            task_queue->wait_dequeue(txn_ptr);
            if(txn_ptr != nullptr) {
                epoch = txn_ptr->commit_epoch();
                epoch_mod = epoch % ctx.kCacheMaxLength;
                /// dequeue from local txn queue a complete txn
                while(epoch_local_txn_queue[epoch_mod]->try_dequeue(txn_ptr)) {
                    commit_queue->enqueue(std::move(txn_ptr));
                }
                commit_queue->enqueue(nullptr);
                while(commit_queue->try_dequeue(txn_ptr)) {
                    if(txn_ptr == nullptr) continue;
                    epoch = txn_ptr->commit_epoch();
                    ///validation phase
                    if (!CRDTMerge::ValidateWriteSet(ctx, *(txn_ptr))) {
                        auto key = std::to_string(txn_ptr->client_txn_id());
                        abort_txn_set.insert(key,key);
                        MessageSendHandler::SendTxnCommitResultToClient(ctx, *(txn_ptr), proto::TxnState::Abort);
                    }
                    else {
                        epoch_record_commit_txn_num.IncCount(epoch, txn_ptr->server_id(), 1);
                        CRDTMerge::Commit(ctx, *(txn_ptr));
                        RedoLoger::RedoLog(ctx, *(txn_ptr));
                        epoch_record_committed_txn_num.IncCount(epoch, txn_ptr->server_id(), 1);
                        MessageSendHandler::SendTxnCommitResultToClient(ctx, *(txn_ptr), proto::TxnState::Commit);
                    }
                    epoch_committed_txn_num.IncCount(epoch, txn_ptr->server_id(), 1);


                    while (epoch_commit_queue[epoch_mod]->try_dequeue(txn_ptr) && txn_ptr != nullptr) {
                        //不对分片事务进行commit处理
                        epoch = txn_ptr->commit_epoch();
                        epoch_committed_txn_num.IncCount(epoch, txn_ptr->server_id(), 1);
                    }

                }
                CheckEpochCommitComplete(ctx, epoch);
            }
        }
    }

    void Merger::EpochCommit_RedoLog_TxnMode_Commit_Queue_Wait() {
        while(!EpochManager::IsTimerStop()) {
            commit_queue->wait_dequeue(txn_ptr);
            if(txn_ptr == nullptr) return;
            epoch = txn_ptr->commit_epoch();
            ///validation phase
            if (!CRDTMerge::ValidateWriteSet(ctx, *(txn_ptr))) {
                auto key = std::to_string(txn_ptr->client_txn_id());
                abort_txn_set.insert(key,key);
                MessageSendHandler::SendTxnCommitResultToClient(ctx, *(txn_ptr), proto::TxnState::Abort);
            }
            else {
                epoch_record_commit_txn_num.IncCount(epoch, txn_ptr->server_id(), 1);
                CRDTMerge::Commit(ctx, *(txn_ptr));
                RedoLoger::RedoLog(ctx, *(txn_ptr));
                epoch_record_committed_txn_num.IncCount(epoch, txn_ptr->server_id(), 1);
                MessageSendHandler::SendTxnCommitResultToClient(ctx, *(txn_ptr), proto::TxnState::Commit);
            }
            epoch_committed_txn_num.IncCount(epoch, txn_ptr->server_id(), 1);

            epoch_mod = epoch % ctx.kCacheMaxLength;
            while (epoch_commit_queue[epoch_mod]->try_dequeue(txn_ptr) && txn_ptr != nullptr) {
                //不对分片事务进行commit处理
                epoch = txn_ptr->commit_epoch();
                epoch_committed_txn_num.IncCount(epoch, txn_ptr->server_id(), 1);
            }

        }
    }

}