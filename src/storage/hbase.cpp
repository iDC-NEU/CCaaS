//
// Created by zwx on 23-6-30.
//

#include "storage/hbase.h"
#include "hbase_server/hbaseHandler.h"
#include "epoch/epoch_manager.h"

namespace Taas {
    Context HBase::ctx;
    AtomicCounters_Cache
            HBase::epoch_should_push_down_txn_num(10, 1), HBase::epoch_pushed_down_txn_num(10, 1);
    std::unique_ptr<moodycamel::BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>  HBase::task_queue, HBase::redo_log_queue;
    std::vector<std::unique_ptr<moodycamel::BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>>
            HBase::epoch_redo_log_queue;
    std::vector<std::unique_ptr<std::atomic<bool>>> HBase::epoch_redo_log_complete;


    void HBase::StaticInit(const Context &ctx_) {
        ctx = ctx_;
        task_queue = std::make_unique<moodycamel::BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>();
        redo_log_queue = std::make_unique<moodycamel::BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>();
        epoch_should_push_down_txn_num.Init(ctx.kCacheMaxLength, ctx.kTxnNodeNum);
        epoch_pushed_down_txn_num.Init(ctx.kCacheMaxLength, ctx.kTxnNodeNum);
        epoch_redo_log_complete.resize(ctx.kCacheMaxLength);
        epoch_redo_log_queue.resize(ctx.kCacheMaxLength);
        for(int i = 0; i < static_cast<int>(ctx.kCacheMaxLength); i ++) {
            epoch_redo_log_complete[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_redo_log_queue[i] = std::make_unique<moodycamel::BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>();
        }
    }

    void HBase::StaticClear(uint64_t &epoch) {
        epoch_should_push_down_txn_num.Clear(epoch);
        epoch_pushed_down_txn_num.Clear(epoch);
        epoch_redo_log_complete[epoch % ctx.kCacheMaxLength]->store(false);
        auto txn_ptr = std::make_unique<proto::Transaction>();
        while(epoch_redo_log_queue[epoch % ctx.kCacheMaxLength]->try_dequeue(txn_ptr));
    }

    bool HBase::GeneratePushDownTask(uint64_t &epoch) {
        auto txn_ptr = std::make_unique<proto::Transaction>();
        txn_ptr->set_commit_epoch(epoch);
        task_queue->enqueue(std::move(txn_ptr));
        task_queue->enqueue(nullptr);
        return true;
    }

    ///YCSB
    const std::string table_name("usertable");
    const std::string family("entire");
    //not clear about the form of row.data(), so insert it as an entire column
    const std::string qualifier("");

    void HBase::SendTransactionToDB_Usleep() {
        std::unique_ptr<proto::Transaction> txn_ptr;
        uint64_t epoch;
        epoch = EpochManager::GetPushDownEpoch();
        //connect to thrift2 server in order to communicate with hbase
        CHbaseHandler hbase_txn;
        hbase_txn.connect(ctx.kHbaseIP,9090);
        while(redo_log_queue->try_dequeue(txn_ptr)) {
            if(txn_ptr == nullptr) continue ;
            for (auto i = 0; i < txn_ptr->row_size(); i++) {
                const auto& row = txn_ptr->row(i);
                if (row.op_type() == proto::OpType::Insert || row.op_type() == proto::OpType::Update) {
                  const std::string key = row.key();
                  const std::string row_value = row.data();
                  hbase_txn.putRow(table_name, key, family, qualifier, row_value);
                }

            }
            epoch_pushed_down_txn_num.IncCount(epoch, txn_ptr->server_id(), 1);
        }
        hbase_txn.disconnect();
    }


    void HBase::SendTransactionToDB_Block() {
        std::unique_ptr<proto::Transaction> txn_ptr;
        uint64_t epoch;
        while(!EpochManager::IsTimerStop()) {
            redo_log_queue->wait_dequeue(txn_ptr);
            if(txn_ptr == nullptr) continue;
            epoch = txn_ptr->commit_epoch();
            /// do like Usleep func
            epoch_pushed_down_txn_num.IncCount(epoch, txn_ptr->server_id(), 1);
        }
    }

    void HBase::DBRedoLogQueueEnqueue(uint64_t &epoch, std::unique_ptr<proto::Transaction> &&txn_ptr) {
        auto epoch_mod = epoch % ctx.kCacheMaxLength;
        epoch_redo_log_queue[epoch_mod]->enqueue(std::move(txn_ptr));
        epoch_redo_log_queue[epoch_mod]->enqueue(nullptr);
    }
    bool HBase::DBRedoLogQueueTryDequeue(uint64_t &epoch, std::unique_ptr<proto::Transaction> &txn_ptr) {
        auto epoch_mod = epoch % ctx.kCacheMaxLength;
        return epoch_redo_log_queue[epoch_mod]->try_dequeue(txn_ptr);
    }

    bool HBase::CheckEpochPushDownComplete(uint64_t &epoch) {
        if(epoch_redo_log_complete[epoch % ctx.kCacheMaxLength]->load()) return true;
        if(epoch < EpochManager::GetLogicalEpoch() &&
           epoch_pushed_down_txn_num.GetCount(epoch) >= epoch_should_push_down_txn_num.GetCount(epoch)) {
            epoch_redo_log_complete[epoch % ctx.kCacheMaxLength]->store(true);
            return true;
        }
        return false;
    }
}
