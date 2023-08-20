//
// Created by zwx on 23-6-30.
//

#include "storage/leveldb.h"
#include "epoch/epoch_manager.h"

#include "proto/kvdb_server.pb.h"
#include <glog/logging.h>

namespace Taas {
    Context LevelDB::ctx;
    std::unique_ptr<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>  LevelDB::task_queue, LevelDB::redo_log_queue;
    std::vector<std::unique_ptr<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>> LevelDB::epoch_redo_log_queue;
    std::atomic<uint64_t> LevelDB::pushed_down_epoch(1);
    AtomicCounters_Cache LevelDB::epoch_should_push_down_txn_num(10, 1), LevelDB::epoch_pushed_down_txn_num(10, 1);
    std::atomic<uint64_t> LevelDB::total_commit_txn_num(0), LevelDB::success_commit_txn_num(0), LevelDB::failed_commit_txn_num(0);
    std::vector<std::unique_ptr<std::atomic<bool>>> LevelDB::epoch_redo_log_complete;
    std::condition_variable LevelDB::commit_cv;

    brpc::Channel LevelDB::channel;

    void LevelDB::StaticInit(const Context &ctx_) {
        ctx = ctx_;
        task_queue = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
        redo_log_queue = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
        epoch_should_push_down_txn_num.Init(ctx.kCacheMaxLength, ctx.kTxnNodeNum);
        epoch_pushed_down_txn_num.Init(ctx.kCacheMaxLength, ctx.kTxnNodeNum);
        epoch_redo_log_complete.resize(ctx.kCacheMaxLength);
        epoch_redo_log_queue.resize(ctx.kCacheMaxLength);
        for(int i = 0; i < static_cast<int>(ctx.kCacheMaxLength); i ++) {
            epoch_redo_log_complete[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_redo_log_queue[i] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
        }
        brpc::ChannelOptions options;
        channel.Init(ctx.kLevevDBIP.c_str(), &options);
    }

    void LevelDB::StaticClear(const uint64_t &epoch) {
        epoch_should_push_down_txn_num.Clear(epoch);
        epoch_pushed_down_txn_num.Clear(epoch);
        epoch_redo_log_complete[epoch % ctx.kCacheMaxLength]->store(false);
        auto txn_ptr = std::make_shared<proto::Transaction>();
        while(epoch_redo_log_queue[epoch % ctx.kCacheMaxLength]->try_dequeue(txn_ptr));
    }

    bool LevelDB::GeneratePushDownTask(const uint64_t &epoch) {
        auto txn_ptr = std::make_shared<proto::Transaction>();
        txn_ptr->set_commit_epoch(epoch);
        task_queue->enqueue(txn_ptr);
        task_queue->enqueue(nullptr);
        return true;
    }

    void LevelDB::SendTransactionToDB_Usleep() {
        brpc::Channel chan;
        brpc::ChannelOptions options;
        chan.Init(ctx.kLevevDBIP.c_str(), &options);
        std::shared_ptr<proto::Transaction> txn_ptr;
        proto::KvDBPutService_Stub put_stub(&chan);
        proto::KvDBGetService_Stub get_stub(&chan);
        bool sleep_flag;
        uint64_t epoch, epoch_mod;
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
                total_commit_txn_num.fetch_add(1);
                auto csn = txn_ptr->csn();
                for(const auto& i : txn_ptr->row()) {
                    if (i.op_type() == proto::OpType::Read) {
                        continue;
                    }
                    proto::KvDBRequest request;
                    proto::KvDBResponse response;
                    brpc::Controller cntl;
                    cntl.set_timeout_ms(500);
                    auto data = request.add_data();
                    data->set_op_type(i.op_type());
                    data->set_key(i.key());
                    data->set_value(i.data());
                    data->set_csn(csn);
                    put_stub.Put(&cntl, &request, &response, nullptr);
                    if (cntl.Failed()) {
                        // RPC失败.
                        failed_commit_txn_num.fetch_add(1);
                        LOG(WARNING) << cntl.ErrorText();
                    } else {
                        // RPC成功
//                        LOG(INFO) << "LevelDBStorageSend success === 0"
//                                  << "Received response from " << cntl.remote_side()
//                                  << " to " << cntl.local_side()
//                                  << ": " << response.result() << " (attached="
//                                  << cntl.response_attachment() << ")"
//                                  << " latency=" << cntl.latency_us() << "us";
                    }
                }
                epoch_pushed_down_txn_num.IncCount(txn_ptr->commit_epoch(), txn_ptr->server_id(), 1);
                sleep_flag = false;
            }
            if(sleep_flag)
                usleep(sleep_time);
        }
    }


    void LevelDB::SendTransactionToDB_Block() {
        brpc::Channel chan;
        brpc::ChannelOptions options;
        chan.Init(ctx.kLevevDBIP.c_str(), &options);
        std::shared_ptr<proto::Transaction> txn_ptr;
        proto::KvDBPutService_Stub put_stub(&chan);
        proto::KvDBGetService_Stub get_stub(&chan);

        std::mutex mtx;
        std::unique_lock lck(mtx);
        uint64_t epoch, epoch_mod;
        bool sleep_flag;
        while(!EpochManager::IsTimerStop()) {
            epoch = EpochManager::GetPushDownEpoch();
            while (!EpochManager::IsCommitComplete(epoch)) {
                commit_cv.wait(lck);
                epoch = EpochManager::GetPushDownEpoch();
            }
            epoch_mod = epoch % ctx.kCacheMaxLength;
            sleep_flag = true;
            while (epoch_redo_log_queue[epoch_mod]->try_dequeue(txn_ptr)) {
                if (txn_ptr == nullptr || txn_ptr->txn_type() == proto::TxnType::NullMark) {
                    continue;
                }
                auto csn = txn_ptr->csn();
                for(const auto& i : txn_ptr->row()) {
                    if (i.op_type() == proto::OpType::Read) {
                        continue;
                    }
                    proto::KvDBRequest request;
                    proto::KvDBResponse response;
                    brpc::Controller cntl;
                    cntl.set_timeout_ms(500);
                    auto data = request.add_data();
                    data->set_op_type(i.op_type());
                    data->set_key(i.key());
                    data->set_value(i.data());
                    data->set_csn(csn);
                    put_stub.Put(&cntl, &request, &response, nullptr);
                    if (cntl.Failed()) {
                        // RPC失败.
                        LOG(WARNING) << cntl.ErrorText();
                    } else {
                        // RPC成功
//                        LOG(INFO) << "LevelDBStorageSend success === 1"
//                                  << "Received response from " << cntl.remote_side()
//                                  << " to " << cntl.local_side()
//                                  << ": " << response.result() << " (attached="
//                                  << cntl.response_attachment() << ")"
//                                  << " latency=" << cntl.latency_us() << "us";
                    }
                }
                epoch_pushed_down_txn_num.IncCount(txn_ptr->commit_epoch(), txn_ptr->server_id(), 1);
                sleep_flag = false;
            }
            if(sleep_flag)
                usleep(sleep_time);
        }
    }

    void LevelDB::DBRedoLogQueueEnqueue(const uint64_t &epoch, std::shared_ptr<proto::Transaction> txn_ptr) {
        epoch_should_push_down_txn_num.IncCount(epoch, txn_ptr->server_id(), 1);
        auto epoch_mod = epoch % ctx.kCacheMaxLength;
        epoch_redo_log_queue[epoch_mod]->enqueue(txn_ptr);
        epoch_redo_log_queue[epoch_mod]->enqueue(nullptr);
    }
    bool LevelDB::DBRedoLogQueueTryDequeue(const uint64_t &epoch, std::shared_ptr<proto::Transaction> txn_ptr) {
        auto epoch_mod = epoch % ctx.kCacheMaxLength;
        return epoch_redo_log_queue[epoch_mod]->try_dequeue(txn_ptr);
    }

    bool LevelDB::CheckEpochPushDownComplete(const uint64_t &epoch) {
        if(epoch_redo_log_complete[epoch % ctx.kCacheMaxLength]->load()) return true;
        if(epoch < EpochManager::GetLogicalEpoch() &&
           epoch_pushed_down_txn_num.GetCount(epoch) >= epoch_should_push_down_txn_num.GetCount(epoch)) {
            epoch_redo_log_complete[epoch % ctx.kCacheMaxLength]->store(true);
            return true;
        }
        return false;
    }

}
