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
            Merger::read_version_map_data, ///read validate for higher isolation
            Merger::read_version_map_csn, ///read validate for higher isolation
            Merger::insert_set,   ///插入集合，用于判断插入是否可以执行成功 check key exits?
            Merger::abort_txn_set; /// 所有abort的事务，不区分epoch

    std::unique_ptr<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>> Merger::task_queue;
    std::unique_ptr<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>> Merger::merge_queue;///存放要进行merge的事务，分片
    std::unique_ptr<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>> Merger::commit_queue;
    std::vector<std::unique_ptr<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>>
            Merger::epoch_merge_queue,///存放要进行merge的事务，分片
            Merger::epoch_local_txn_queue,///存放epoch由client发送过来的事务，存放每个epoch要进行写日志的事务，整个事务写日志
            Merger::epoch_commit_queue;///存放每个epoch要进行写日志的事务，分片写日志

    std::vector<std::unique_ptr<std::atomic<bool>>>
            Merger::epoch_merge_complete,
            Merger::epoch_commit_complete;

    std::atomic<uint64_t> Merger::total_merge_txn_num(0), Merger::total_merge_latency(0), Merger::total_commit_txn_num(0),
        Merger::total_commit_latency(0), Merger::success_commit_txn_num(0), Merger::success_commit_latency(0),
        Merger::total_read_version_check_failed_txn_num(0), Merger::total_failed_txn_num(0);

    std::mutex Merger::merge_mutex, Merger::commit_mutex;
    std::condition_variable Merger::merge_cv, Merger::commit_cv;


    void Merger::StaticInit(const Context &ctx) {
        auto max_length = ctx.kCacheMaxLength;
        auto pack_num = ctx.kIndexNum;
        ///epoch merge state
        epoch_merge_map.resize(max_length);
        local_epoch_abort_txn_set.resize(max_length);
        epoch_abort_txn_set.resize(max_length);
        epoch_insert_set.resize(max_length);

//        ///transaction concurrent queue
        task_queue = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
        merge_queue = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
        commit_queue = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();

        epoch_merge_queue.resize(max_length);
        epoch_commit_queue.resize(max_length);
//        epoch_commit_queue.resize(max_length);

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

            epoch_merge_queue[i] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
            epoch_commit_queue[i] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
//            epoch_commit_queue[i] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
        }
        RedoLoger::StaticInit(ctx);
    }

    void Merger::MergeQueueEnqueue(const Context &ctx, uint64_t &epoch, std::shared_ptr<proto::Transaction> txn_ptr) {
        auto epoch_mod = epoch % ctx.kCacheMaxLength;
        epoch_should_merge_txn_num.IncCount(epoch, txn_ptr->server_id(), 1);
        epoch_merge_queue[epoch_mod]->enqueue(txn_ptr);
        epoch_merge_queue[epoch_mod]->enqueue(nullptr);
    }
    bool Merger::MergeQueueTryDequeue(const Context &ctx, uint64_t &epoch, std::shared_ptr<proto::Transaction> txn_ptr) {
        ///not use for now
        return false;
    }
    void Merger::CommitQueueEnqueue(const Context& ctx, uint64_t& epoch, std::shared_ptr<proto::Transaction> txn_ptr) {
        epoch_should_commit_txn_num.IncCount(epoch, ctx.txn_node_ip_index, 1);
        auto epoch_mod = epoch % ctx.kCacheMaxLength;
        epoch_commit_queue[epoch_mod]->enqueue(txn_ptr);
        epoch_commit_queue[epoch_mod]->enqueue(nullptr);
    }
    bool Merger::CommitQueueTryDequeue(const Context& ctx, uint64_t& epoch, std::shared_ptr<proto::Transaction> txn_ptr) {
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

    void Merger::Merge() {
        auto time1 = now_to_us();
        epoch = txn_ptr->commit_epoch();
        if (!CRDTMerge::ValidateReadSet(ctx, txn_ptr)) {
            total_read_version_check_failed_txn_num.fetch_add(1);
            goto end;
        }
        if (!CRDTMerge::MultiMasterCRDTMerge(ctx, txn_ptr)) {
            goto end;
        }
        end:
        total_merge_txn_num.fetch_add(1);
        total_merge_latency.fetch_add(now_to_us() - time1);
        epoch_merged_txn_num.IncCount(epoch, txn_server_id, 1);
    }

    void Merger::Commit() {
        auto time1 = now_to_us();
        ///validation phase
        if (!CRDTMerge::ValidateWriteSet(ctx, txn_ptr)) {
            auto key = std::to_string(txn_ptr->client_txn_id());
            abort_txn_set.insert(key, key);
            total_failed_txn_num.fetch_add(1);
            EpochMessageSendHandler::SendTxnCommitResultToClient(ctx, txn_ptr, proto::TxnState::Abort);
        } else {
            epoch_record_commit_txn_num.IncCount(epoch, txn_ptr->server_id(), 1);
            CRDTMerge::Commit(ctx, txn_ptr);
            if(txn_ptr->server_id() == ctx.txn_node_ip_index) { /// only local txn do redo log
                RedoLoger::RedoLog(ctx, txn_ptr);
            }
            epoch_record_committed_txn_num.IncCount(epoch, txn_ptr->server_id(), 1);
            success_commit_txn_num.fetch_add(1);
            success_commit_latency.fetch_add(now_to_us() - time1);
            EpochMessageSendHandler::SendTxnCommitResultToClient(ctx, txn_ptr, proto::TxnState::Commit);
        }
        total_commit_txn_num.fetch_add(1);
        total_commit_latency.fetch_add(now_to_us() - time1);
        epoch_committed_txn_num.IncCount(epoch, txn_ptr->server_id(), 1);
    }

    void Merger::EpochMerge() {
        epoch = EpochManager::GetLogicalEpoch();
        std::mutex mtx;
        std::unique_lock lck(mtx);
        while (!EpochManager::IsTimerStop()) {
            epoch = EpochManager::GetLogicalEpoch();
            epoch_mod = epoch % ctx.kCacheMaxLength;
            while (!EpochManager::IsShardingMergeComplete(epoch)) {
                while(epoch_merge_queue[epoch_mod]->try_dequeue(txn_ptr)) {
                    if (txn_ptr != nullptr && txn_ptr->txn_type() != proto::TxnType::NullMark) {
                        Merge();
                    }
                }
                usleep(merge_sleep_time);
                epoch = EpochManager::GetLogicalEpoch();
                epoch_mod = epoch % ctx.kCacheMaxLength;
            }
            while (!EpochManager::IsAbortSetMergeComplete(epoch)) {
                usleep(merge_sleep_time);
                epoch = EpochManager::GetLogicalEpoch();
                epoch_mod = epoch % ctx.kCacheMaxLength;
            }
            while (!EpochManager::IsCommitComplete(epoch)) {
                while(epoch_commit_queue[epoch_mod]->try_dequeue(txn_ptr)) {
                    if (txn_ptr != nullptr && txn_ptr->txn_type() != proto::TxnType::NullMark) {
                        Commit();
                    }
                }
                usleep(merge_sleep_time);
                epoch = EpochManager::GetLogicalEpoch();
                epoch_mod = epoch % ctx.kCacheMaxLength;
            }
        }


    void Merger::EpochMerge_Usleep() {
        while (!EpochManager::IsTimerStop()) {
            epoch = EpochManager::GetLogicalEpoch();
            while(EpochManager::IsShardingMergeComplete(epoch)) {
                usleep(merge_sleep_time);
                epoch = EpochManager::GetLogicalEpoch();
            }
            epoch_mod = epoch % ctx.kCacheMaxLength;
            sleep_flag = true;
            while(epoch_merge_queue[epoch_mod]->try_dequeue(txn_ptr)) {
                if(txn_ptr == nullptr || txn_ptr->txn_type() == proto::TxnType::NullMark) {
                    continue;
                }
                merge_cv.notify_all();
                auto time1 = now_to_us();
                epoch = txn_ptr->commit_epoch();
                if (!CRDTMerge::ValidateReadSet(ctx, txn_ptr)) {
                    total_read_version_check_failed_txn_num.fetch_add(1);
                    goto end;
                }
                if (!CRDTMerge::MultiMasterCRDTMerge(ctx, txn_ptr)) {
                    goto end;
                }
                end:
                total_merge_txn_num.fetch_add(1);
                total_merge_latency.fetch_add(now_to_us() - time1);
                epoch_merged_txn_num.IncCount(epoch, txn_server_id, 1);
                sleep_flag = false;
            }

            if(sleep_flag) {
                usleep(merge_sleep_time);
            }
        }
    }

    void Merger::EpochMerge_Block() {
        std::mutex mtx;
        std::unique_lock lck(mtx);
        while (!EpochManager::IsTimerStop()) {
            epoch = EpochManager::GetLogicalEpoch();
            while(EpochManager::IsShardingMergeComplete(epoch)) {
                merge_cv.wait(lck);
                epoch = EpochManager::GetLogicalEpoch();
            }
            epoch_mod = epoch % ctx.kCacheMaxLength;
            sleep_flag = true;
            while(epoch_merge_queue[epoch_mod]->try_dequeue(txn_ptr)) {
                if(txn_ptr == nullptr || txn_ptr->txn_type() == proto::TxnType::NullMark) {
                    continue;
                }
                auto time1 = now_to_us();
                epoch = txn_ptr->commit_epoch();
                if (!CRDTMerge::ValidateReadSet(ctx, txn_ptr)) {
                    total_read_version_check_failed_txn_num.fetch_add(1);
                    goto end;
                }
                if (!CRDTMerge::MultiMasterCRDTMerge(ctx, txn_ptr)) {
                    goto end;
                }
                end:
                total_merge_txn_num.fetch_add(1);
                total_merge_latency.fetch_add(now_to_us() - time1);
                epoch_merged_txn_num.IncCount(epoch, txn_server_id, 1);
                sleep_flag = false;
            }
            if(sleep_flag) {
                usleep(merge_sleep_time);
            }
        }
    }

    void Merger::EpochCommit_Usleep() {
        while (!EpochManager::IsTimerStop()) {
            epoch = EpochManager::GetLogicalEpoch();
            while(!EpochManager::IsAbortSetMergeComplete(epoch)) {
                usleep(merge_sleep_time);
                epoch = EpochManager::GetLogicalEpoch();
            }
            epoch_mod = epoch % ctx.kCacheMaxLength;
            sleep_flag = true;
            while(epoch_commit_queue[epoch_mod]->try_dequeue(txn_ptr)) {
                if(txn_ptr == nullptr || txn_ptr->txn_type() == proto::TxnType::NullMark) {
                    continue;
                }
                commit_cv.notify_all();
                auto time1 = now_to_us();
                ///validation phase
                if (!CRDTMerge::ValidateWriteSet(ctx, txn_ptr)) {
                    auto key = std::to_string(txn_ptr->client_txn_id());
                    abort_txn_set.insert(key, key);
                    total_failed_txn_num.fetch_add(1);
                    EpochMessageSendHandler::SendTxnCommitResultToClient(ctx, txn_ptr, proto::TxnState::Abort);
                } else {
                    epoch_record_commit_txn_num.IncCount(epoch, txn_ptr->server_id(), 1);
                    CRDTMerge::Commit(ctx, txn_ptr);
                    if(txn_ptr->server_id() == ctx.txn_node_ip_index) { /// only local txn do redo log
                        RedoLoger::RedoLog(ctx, txn_ptr);
                    }
                    epoch_record_committed_txn_num.IncCount(epoch, txn_ptr->server_id(), 1);
                    success_commit_txn_num.fetch_add(1);
                    success_commit_latency.fetch_add(now_to_us() - time1);
                    EpochMessageSendHandler::SendTxnCommitResultToClient(ctx, txn_ptr, proto::TxnState::Commit);
                }
                total_commit_txn_num.fetch_add(1);
                total_commit_latency.fetch_add(now_to_us() - time1);
                epoch_committed_txn_num.IncCount(epoch, txn_ptr->server_id(), 1);
                sleep_flag = false;
            }
            if(sleep_flag) {
                usleep(merge_sleep_time);
            }
        }
    }

    void Merger::EpochCommit_Block() {
        std::mutex mtx;
        std::unique_lock lck(mtx);
        while (!EpochManager::IsTimerStop()) {
            epoch = EpochManager::GetLogicalEpoch();
            while(!EpochManager::IsAbortSetMergeComplete(epoch)) {
                commit_cv.wait(lck);
                epoch = EpochManager::GetLogicalEpoch();
            }
            sleep_flag = true;
            epoch_mod = epoch % ctx.kCacheMaxLength;
            while(epoch_commit_queue[epoch_mod]->try_dequeue(txn_ptr)) {
                if(txn_ptr == nullptr || txn_ptr->txn_type() == proto::TxnType::NullMark) {
                    continue;
                }
                auto time1 = now_to_us();
                ///validation phase
                if (!CRDTMerge::ValidateWriteSet(ctx, txn_ptr)) {
                    auto key = std::to_string(txn_ptr->client_txn_id());
                    abort_txn_set.insert(key, key);
                    total_failed_txn_num.fetch_add(1);
                    EpochMessageSendHandler::SendTxnCommitResultToClient(ctx, txn_ptr, proto::TxnState::Abort);
                } else {
                    epoch_record_commit_txn_num.IncCount(epoch, txn_ptr->server_id(), 1);
                    CRDTMerge::Commit(ctx, txn_ptr);
                    if(txn_ptr->server_id() == ctx.txn_node_ip_index) { /// only local txn do redo log
                        RedoLoger::RedoLog(ctx, txn_ptr);
                    }
                    epoch_record_committed_txn_num.IncCount(epoch, txn_ptr->server_id(), 1);
                    success_commit_txn_num.fetch_add(1);
                    success_commit_latency.fetch_add(now_to_us() - time1);
                    EpochMessageSendHandler::SendTxnCommitResultToClient(ctx, txn_ptr, proto::TxnState::Commit);
                }
                total_commit_txn_num.fetch_add(1);
                total_commit_latency.fetch_add(now_to_us() - time1);
                epoch_committed_txn_num.IncCount(epoch, txn_ptr->server_id(), 1);
                sleep_flag = false;
            }
            if(sleep_flag) {
                usleep(merge_sleep_time);
            }

//            commit_queue->wait_dequeue(txn_ptr);
//            if (txn_ptr == nullptr || txn_ptr->txn_type() == proto::TxnType::NullMark) {
//                continue;
//            }
//            epoch = txn_ptr->commit_epoch();
//            auto time1 = now_to_us();
//            ///validation phase
//            if (!CRDTMerge::ValidateWriteSet(ctx, *(txn_ptr))) {
//                auto key = std::to_string(txn_ptr->client_txn_id());
//                abort_txn_set.insert(key, key);
//                total_failed_txn_num.fetch_add(1);
//                EpochMessageSendHandler::SendTxnCommitResultToClient(ctx, *(txn_ptr), proto::TxnState::Abort);
//            } else {
//                epoch_record_commit_txn_num.IncCount(epoch, txn_ptr->server_id(), 1);
//                CRDTMerge::Commit(ctx, *(txn_ptr));
//                if(txn_ptr->server_id() == ctx.txn_node_ip_index) { /// only local txn do redo log
//                    RedoLoger::RedoLog(ctx, *(txn_ptr));
//                }
//                epoch_record_committed_txn_num.IncCount(epoch, txn_ptr->server_id(), 1);
//                success_commit_txn_num.fetch_add(1);
//                success_commit_latency.fetch_add(now_to_us() - time1);
//                EpochMessageSendHandler::SendTxnCommitResultToClient(ctx, *(txn_ptr), proto::TxnState::Commit);
//            }
//            total_commit_txn_num.fetch_add(1);
//            total_commit_latency.fetch_add(now_to_us() - time1);
//            epoch_committed_txn_num.IncCount(epoch, txn_ptr->server_id(), 1);
        }
    }

/////          不对分片事务进行commit处理
//            epoch_mod = epoch % ctx.kCacheMaxLength;
//            while (epoch_commit_queue[epoch_mod]->try_dequeue(txn_ptr) && txn_ptr != nullptr) {
//                epoch = txn_ptr->commit_epoch();
//                epoch_committed_txn_num.IncCount(epoch, txn_ptr->server_id(), 1);
//            }

}