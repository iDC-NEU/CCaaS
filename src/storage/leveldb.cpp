//
// Created by zwx on 23-6-30.
//

#include "storage/leveldb.h"
#include "epoch/epoch_manager.h"

#include "proto/kvdb_server.pb.h"

namespace Taas {
    Context LevelDB::ctx;
    AtomicCounters_Cache
            LevelDB::epoch_should_push_down_txn_num(10, 1), LevelDB::epoch_pushed_down_txn_num(10, 1);
    std::unique_ptr<moodycamel::BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>  LevelDB::task_queue, LevelDB::redo_log_queue;
    std::vector<std::unique_ptr<moodycamel::BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>>
            LevelDB::epoch_redo_log_queue;
    std::vector<std::unique_ptr<std::atomic<bool>>> LevelDB::epoch_redo_log_complete;

    brpc::Channel LevelDB::channel;


    void LevelDB::StaticInit(const Context &ctx_) {
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
        brpc::ChannelOptions options;
        LevelDB::channel.Init(ctx.kLevevDBIP.c_str(), &options);
    }

    void LevelDB::StaticClear(uint64_t &epoch) {
        epoch_should_push_down_txn_num.Clear(epoch);
        epoch_pushed_down_txn_num.Clear(epoch);
        epoch_redo_log_complete[epoch % ctx.kCacheMaxLength]->store(false);
        auto txn_ptr = std::make_unique<proto::Transaction>();
        while(epoch_redo_log_queue[epoch % ctx.kCacheMaxLength]->try_dequeue(txn_ptr));
    }

    bool LevelDB::GeneratePushDownTask(uint64_t &epoch) {
        auto txn_ptr = std::make_unique<proto::Transaction>();
        txn_ptr->set_commit_epoch(epoch);
        task_queue->enqueue(std::move(txn_ptr));
        task_queue->enqueue(nullptr);
        return true;
    }

    void LevelDB::SendTransactionToDB_Usleep() {
        std::unique_ptr<proto::Transaction> txn_ptr;
        proto::KvDBPutService_Stub put_stub(&channel);
        proto::KvDBGetService_Stub get_stub(&channel);
        uint64_t epoch;
        epoch = EpochManager::GetPushDownEpoch();
        while(redo_log_queue->try_dequeue(txn_ptr)) {
            if(txn_ptr == nullptr) continue ;
            proto::KvDBRequest request;
            proto::KvDBResponse response;
            brpc::Controller cntl;
            cntl.set_timeout_ms(500);
            auto csn = txn_ptr->csn();
            for(auto i : txn_ptr->row()) {
                if(i.op_type() == proto::OpType::Read) {
                    continue;
                }
                auto data = request.add_data();
                data->set_op_type(i.op_type());
                data->set_key(i.key());
                data->set_value(i.data());
                data->set_csn(csn);
                put_stub.Put(&cntl, &request, &response, NULL);
                if (cntl.Failed()) {
                    // RPC失败了. response里的值是未定义的，勿用。
                } else {
                    // RPC成功了，response里有我们想要的回复数据。
                }
            }
            epoch_pushed_down_txn_num.IncCount(epoch, txn_ptr->server_id(), 1);
        }
    }


    void LevelDB::SendTransactionToDB_Block() {
        std::unique_ptr<proto::Transaction> txn_ptr;
        uint64_t epoch;
        while(!EpochManager::IsTimerStop()) {
            redo_log_queue->wait_dequeue(txn_ptr);
            if(txn_ptr == nullptr) continue;
            epoch = txn_ptr->commit_epoch();
//            if(tikv_client_ptr == nullptr) continue;
//            auto tikv_txn = tikv_client_ptr->begin();
//            for (auto i = 0; i < txn_ptr->row_size(); i++) {
//                const auto& row = txn_ptr->row(i);
//                if (row.op_type() == proto::OpType::Insert || row.op_type() == proto::OpType::Update) {
//                    tikv_txn.put(row.key(), row.data());
//                }
//            }
//            tikv_txn.commit();
            epoch_pushed_down_txn_num.IncCount(epoch, txn_ptr->server_id(), 1);
        }
    }

    void LevelDB::DBRedoLogQueueEnqueue(uint64_t &epoch, std::unique_ptr<proto::Transaction> &&txn_ptr) {
        auto epoch_mod = epoch % ctx.kCacheMaxLength;
        epoch_redo_log_queue[epoch_mod]->enqueue(std::move(txn_ptr));
        epoch_redo_log_queue[epoch_mod]->enqueue(nullptr);
    }
    bool LevelDB::DBRedoLogQueueTryDequeue(uint64_t &epoch, std::unique_ptr<proto::Transaction> &txn_ptr) {
        auto epoch_mod = epoch % ctx.kCacheMaxLength;
        return epoch_redo_log_queue[epoch_mod]->try_dequeue(txn_ptr);
    }

    bool LevelDB::CheckEpochPushDownComplete(uint64_t &epoch) {
        if(epoch_redo_log_complete[epoch % ctx.kCacheMaxLength]->load()) return true;
        if(epoch < EpochManager::GetLogicalEpoch() &&
           epoch_pushed_down_txn_num.GetCount(epoch) >= epoch_should_push_down_txn_num.GetCount(epoch)) {
            epoch_redo_log_complete[epoch % ctx.kCacheMaxLength]->store(true);
            return true;
        }
        return false;
    }

}
