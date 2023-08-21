//
// Created by zwx on 23-6-30.
//

#include "storage/hbase.h"
#include "hbase_server/hbaseHandler.h"
#include "epoch/epoch_manager.h"

#include <glog/logging.h>

namespace Taas {
    Context HBase::ctx;
    std::unique_ptr<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>  HBase::task_queue, HBase::redo_log_queue;
    std::vector<std::unique_ptr<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>> HBase::epoch_redo_log_queue;
    std::atomic<uint64_t> HBase::pushed_down_epoch(1);
    AtomicCounters_Cache HBase::epoch_should_push_down_txn_num(10, 1), HBase::epoch_pushed_down_txn_num(10, 1);
    std::atomic<uint64_t> HBase::total_commit_txn_num(0), HBase::success_commit_txn_num(0), HBase::failed_commit_txn_num(0);
    std::vector<std::unique_ptr<std::atomic<bool>>> HBase::epoch_redo_log_complete;
    std::condition_variable HBase::commit_cv;


    void HBase::StaticInit(const Context &ctx_) {
        ctx = ctx_;
        task_queue = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
        redo_log_queue = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
        pushed_down_epoch.store(1);
        epoch_should_push_down_txn_num.Init(ctx.kCacheMaxLength, ctx.kTxnNodeNum);
        epoch_pushed_down_txn_num.Init(ctx.kCacheMaxLength, ctx.kTxnNodeNum);
        epoch_redo_log_complete.resize(ctx.kCacheMaxLength);
        epoch_redo_log_queue.resize(ctx.kCacheMaxLength);
        for(int i = 0; i < static_cast<int>(ctx.kCacheMaxLength); i ++) {
            epoch_redo_log_complete[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_redo_log_queue[i] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
        }
    }

    void HBase::StaticClear(const uint64_t &epoch) {
        epoch_should_push_down_txn_num.Clear(epoch);
        epoch_pushed_down_txn_num.Clear(epoch);
        epoch_redo_log_complete[epoch % ctx.kCacheMaxLength]->store(false);
        epoch_redo_log_queue[epoch % ctx.kCacheMaxLength] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
    }

    bool HBase::GeneratePushDownTask(const uint64_t &epoch) {
        auto txn_ptr = std::make_shared<proto::Transaction>();
        txn_ptr->set_commit_epoch(epoch);
        task_queue->enqueue(txn_ptr);
        task_queue->enqueue(nullptr);
        return true;
    }

    ///YCSB
    const std::string table_name("usertable");
    const std::string family("entire");
    //not clear about the form of row.data(), so insert it as an entire column
    const std::string qualifier("");

    void HBase::SendTransactionToDB_Usleep() {
        bool sleep_flag;
        std::shared_ptr<proto::Transaction> txn_ptr;
        uint64_t epoch, epoch_mod;
        //connect to thrift2 server in order to communicate with hbase
        CHbaseHandler hbase_txn;
        hbase_txn.connect(ctx.kHbaseIP,9090);
        while(!EpochManager::IsTimerStop()) {
            epoch = EpochManager::GetPushDownEpoch();
            while(!EpochManager::IsCommitComplete(epoch)) {
                usleep(storage_sleep_time);
                epoch = EpochManager::GetPushDownEpoch();
            }
            epoch_mod = epoch % ctx.kCacheMaxLength;
            sleep_flag = true;
            while(epoch_redo_log_queue[epoch_mod]->try_dequeue(txn_ptr)) {
                if(txn_ptr == nullptr || txn_ptr->txn_type() == proto::TxnType::NullMark) {
                    continue;
                }
//                commit_cv.notify_all();
                try{
                    total_commit_txn_num.fetch_add(1);
                    for (auto i = 0; i < txn_ptr->row_size(); i++) {
                        const auto& row = txn_ptr->row(i);
                        if (row.op_type() == proto::OpType::Insert || row.op_type() == proto::OpType::Update) {
                            const std::string key = row.key();
                            const std::string row_value = row.data();
                            hbase_txn.putRow(table_name, key, family, qualifier, row_value);
                        }

                    }
                }
                catch (std::exception &e) {
                    LOG(INFO) << "*** Commit Txn To Hbase Failed: " << e.what();
                    failed_commit_txn_num.fetch_add(1);
                }
                epoch_pushed_down_txn_num.IncCount(txn_ptr->commit_epoch(), txn_ptr->server_id(), 1);
                txn_ptr.reset();
                sleep_flag = false;
            }
            if(sleep_flag)
                usleep(storage_sleep_time);
        }
        hbase_txn.disconnect();
    }


    void HBase::SendTransactionToDB_Block() {
        std::shared_ptr<proto::Transaction> txn_ptr;
        CHbaseHandler hbase_txn;
        hbase_txn.connect(ctx.kHbaseIP,9090);
        std::mutex mtx;
        std::unique_lock lck(mtx);
        uint64_t epoch, epoch_mod;
        bool sleep_flag;
        while(!EpochManager::IsTimerStop()) {
            epoch = EpochManager::GetPushDownEpoch();
            while(!EpochManager::IsCommitComplete(epoch)) {
                commit_cv.wait(lck);
                epoch = EpochManager::GetPushDownEpoch();
            }
            epoch_mod = epoch % ctx.kCacheMaxLength;
            sleep_flag = true;
            while(epoch_redo_log_queue[epoch_mod]->try_dequeue(txn_ptr)) {
                if (txn_ptr == nullptr || txn_ptr->txn_type() == proto::TxnType::NullMark) {
                    continue;
                }
                try {
                    total_commit_txn_num.fetch_add(1);
                    for (auto i = 0; i < txn_ptr->row_size(); i++) {
                        const auto &row = txn_ptr->row(i);
                        if (row.op_type() == proto::OpType::Insert || row.op_type() == proto::OpType::Update) {
                            const std::string key = row.key();
                            const std::string row_value = row.data();
                            hbase_txn.putRow(table_name, key, family, qualifier, row_value);
                        }

                    }
                }
                catch (std::exception &e) {
                    LOG(INFO) << "*** Commit Txn To Hbase Failed: " << e.what();
                    failed_commit_txn_num.fetch_add(1);
                }
                epoch_pushed_down_txn_num.IncCount(txn_ptr->commit_epoch(), txn_ptr->server_id(), 1);
                txn_ptr.reset();
                sleep_flag = false;
            }
            if(sleep_flag)
                usleep(storage_sleep_time);
        }
    }

    void HBase::DBRedoLogQueueEnqueue(const uint64_t &epoch, std::shared_ptr<proto::Transaction> txn_ptr) {
        epoch_should_push_down_txn_num.IncCount(epoch, txn_ptr->server_id(), 1);
        auto epoch_mod = epoch % ctx.kCacheMaxLength;
        epoch_redo_log_queue[epoch_mod]->enqueue(txn_ptr);
        epoch_redo_log_queue[epoch_mod]->enqueue(nullptr);
    }
    bool HBase::DBRedoLogQueueTryDequeue(const uint64_t &epoch, std::shared_ptr<proto::Transaction> txn_ptr) {
        auto epoch_mod = epoch % ctx.kCacheMaxLength;
        return epoch_redo_log_queue[epoch_mod]->try_dequeue(txn_ptr);
    }

    bool HBase::CheckEpochPushDownComplete(const uint64_t &epoch) {
        if(epoch_redo_log_complete[epoch % ctx.kCacheMaxLength]->load()) return true;
        if(epoch < EpochManager::GetLogicalEpoch() &&
           epoch_pushed_down_txn_num.GetCount(epoch) >= epoch_should_push_down_txn_num.GetCount(epoch)) {
            epoch_redo_log_complete[epoch % ctx.kCacheMaxLength]->store(true);
            return true;
        }
        return false;
    }
}
