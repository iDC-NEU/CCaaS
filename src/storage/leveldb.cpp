//
// Created by zwx on 23-6-30.
//

#include "storage/leveldb.h"
#include "epoch/epoch_manager.h"

#include "proto/kvdb_server.pb.h"
#include <glog/logging.h>

namespace Taas {

    std::unique_ptr<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>  LevelDB::task_queue, LevelDB::redo_log_queue;
    std::vector<std::unique_ptr<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>> LevelDB::epoch_redo_log_queue;
    std::atomic<uint64_t> LevelDB::pushed_down_epoch(1);
    std::atomic<uint64_t> LevelDB::total_commit_txn_num(0), LevelDB::success_commit_txn_num(0), LevelDB::failed_commit_txn_num(0);
    std::vector<std::unique_ptr<std::atomic<bool>>> LevelDB::epoch_redo_log_complete;
    std::condition_variable LevelDB::commit_cv;

    brpc::Channel LevelDB::channel;

    std::atomic<uint64_t> LevelDB::inc_id;

    std::vector<std::shared_ptr<AtomicCounters_Cache>>
            LevelDB::epoch_should_push_down_txn_num_local_vec,
            LevelDB::epoch_pushed_down_txn_num_local_vec;

    void LevelDB::Init() {
        thread_id = inc_id.fetch_add(1);
        shard_num = TaasContext::kTxnNodeNum;
        max_length = TaasContext::kCacheMaxLength;
        local_server_id = TaasContext::txn_node_ip_index;
        epoch_should_push_down_txn_num_local= std::make_shared<AtomicCounters_Cache>(max_length, shard_num);
        epoch_pushed_down_txn_num_local= std::make_shared<AtomicCounters_Cache>(max_length, shard_num);
        epoch_should_push_down_txn_num_local_vec[thread_id] = epoch_should_push_down_txn_num_local;
        epoch_pushed_down_txn_num_local_vec[thread_id] = epoch_pushed_down_txn_num_local;
    }

    void LevelDB::StaticInit() {

        task_queue = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
        redo_log_queue = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
        epoch_redo_log_complete.resize(TaasContext::kCacheMaxLength);
        epoch_redo_log_queue.resize(TaasContext::kCacheMaxLength);
        for(int i = 0; i < static_cast<int>(TaasContext::kCacheMaxLength); i ++) {
            epoch_redo_log_complete[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_redo_log_queue[i] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
        }
        brpc::ChannelOptions options;
        channel.Init(StorageContext::kLevelDBIP.c_str(), &options);
        epoch_should_push_down_txn_num_local_vec.resize(StorageContext::kLeveldbThreadNum);
        epoch_pushed_down_txn_num_local_vec.resize(StorageContext::kLeveldbThreadNum);
    }

    void LevelDB::StaticClear(const uint64_t &epoch) {
        epoch_redo_log_complete[epoch % TaasContext::kCacheMaxLength]->store(false);
//        epoch_redo_log_queue[epoch % TaasContext::kCacheMaxLength] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
        ClearAllThreadLocalCountNum(epoch, epoch_should_push_down_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, epoch_pushed_down_txn_num_local_vec);
    }

    void LevelDB::ClearAllThreadLocalCountNum(const uint64_t &epoch, const std::vector<std::shared_ptr<AtomicCounters_Cache>> &vec) {
        for(const auto& i : vec) {
            if(i != nullptr)
                i->Clear(epoch);
        }
    }
    uint64_t LevelDB::GetAllThreadLocalCountNum(const uint64_t &epoch, const std::vector<std::shared_ptr<AtomicCounters_Cache>> &vec) {
        uint64_t ans = 0;
        for(const auto& i : vec) {
            if(i != nullptr)
                ans += i->GetCount(epoch);
        }
        return ans;
    }
    uint64_t LevelDB::GetAllThreadLocalCountNum(const uint64_t &epoch, const uint64_t &shard_id, const std::vector<std::shared_ptr<AtomicCounters_Cache>> &vec) {
        uint64_t ans = 0;
        for(const auto& i : vec) {
            if(i != nullptr)
                ans += i->GetCount(epoch, shard_id);
        }
        return ans;
    }

    bool LevelDB::CheckEpochPushDownComplete(const uint64_t &epoch) {
        if(epoch_redo_log_complete[epoch % TaasContext::kCacheMaxLength]->load()) return true;
//        if(epoch < EpochManager::GetLogicalEpoch() &&
//           epoch_pushed_down_txn_num_local->GetCount(epoch) >= epoch_should_push_down_txn_num.GetCount(epoch)) {
//            epoch_redo_log_complete[epoch % TaasContext::kCacheMaxLength]->store(true);
//            return true;
//        }
        if(epoch < EpochManager::GetLogicalEpoch() &&
            GetAllThreadLocalCountNum(epoch, epoch_pushed_down_txn_num_local_vec) >=
            GetAllThreadLocalCountNum(epoch, epoch_should_push_down_txn_num_local_vec)
                ) {
            epoch_redo_log_complete[epoch % TaasContext::kCacheMaxLength]->store(true);
            return true;
        }
        return false;
    }
    void LevelDB::DBRedoLogQueueEnqueue(const uint64_t& thread_id, const uint64_t &epoch, std::shared_ptr<proto::Transaction> txn_ptr) {
        epoch_should_push_down_txn_num_local_vec[thread_id % inc_id.load() ]->IncCount(epoch, txn_ptr->txn_server_id(), 1);
//        epoch_should_push_down_txn_num_local->IncCount(epoch, txn_ptr->txn_txn_server_id(), 1);
        auto epoch_mod = epoch % TaasContext::kCacheMaxLength;
        epoch_redo_log_queue[epoch_mod]->enqueue(txn_ptr);
        epoch_redo_log_queue[epoch_mod]->enqueue(nullptr);
        txn_ptr.reset();
    }

    bool LevelDB::DBRedoLogQueueTryDequeue(const uint64_t &epoch, std::shared_ptr<proto::Transaction> txn_ptr) {
        auto epoch_mod = epoch % TaasContext::kCacheMaxLength;
        return epoch_redo_log_queue[epoch_mod]->try_dequeue(txn_ptr);
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
        chan.Init(StorageContext::kLevelDBIP.c_str(), &options);
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
            epoch_mod = epoch % TaasContext::kCacheMaxLength;
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
                epoch_pushed_down_txn_num_local->IncCount(txn_ptr->commit_epoch(), txn_ptr->txn_server_id(), 1);
                txn_ptr.reset();
                sleep_flag = false;
            }
            if(sleep_flag)
                usleep(storage_sleep_time);
        }
    }


    void LevelDB::SendTransactionToDB_Block() {
        brpc::Channel chan;
        brpc::ChannelOptions options;
        chan.Init(StorageContext::kLevelDBIP.c_str(), &options);
        std::shared_ptr<proto::Transaction> txn_ptr;
        proto::KvDBPutService_Stub put_stub(&chan);
        proto::KvDBGetService_Stub get_stub(&chan);

        std::mutex mtx;
        std::unique_lock lck(mtx);
        uint64_t epoch, epoch_mod;
        bool sleep_flag;
        while(!EpochManager::IsTimerStop()) {
            epoch = EpochManager::GetPushDownEpoch();
            while (!EpochManager::IsRecordCommitted(epoch)) {
                commit_cv.wait(lck);
                epoch = EpochManager::GetPushDownEpoch();
            }
            epoch_mod = epoch % TaasContext::kCacheMaxLength;
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
                epoch_pushed_down_txn_num_local->IncCount(txn_ptr->commit_epoch(), txn_ptr->txn_server_id(), 1);
                txn_ptr.reset();
                sleep_flag = false;
            }
            if(sleep_flag)
                usleep(storage_sleep_time);
        }
    }
}
