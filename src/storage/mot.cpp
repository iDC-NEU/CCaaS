//
// Created by zwx on 23-4-9.
//
#include "storage/mot.h"
#include "tools/utilities.h"
#include "message/message.h"
#include "epoch/epoch_manager.h"

namespace Taas {

    std::unique_ptr<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>  MOT::task_queue, MOT::redo_log_queue;
    std::vector<std::unique_ptr<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>> MOT::epoch_redo_log_queue;
    std::atomic<uint64_t> MOT::pushed_down_epoch(1);
    std::atomic<uint64_t> MOT::total_commit_txn_num(0), MOT::success_commit_txn_num(0), MOT::failed_commit_txn_num(0);
    std::vector<std::unique_ptr<std::atomic<bool>>> MOT::epoch_redo_log_complete;
    std::condition_variable MOT::commit_cv;

    std::atomic<uint64_t> MOT::inc_id;

    std::vector<std::shared_ptr<AtomicCounters_Cache>>
            MOT::epoch_should_push_down_txn_num_local_vec,
            MOT::epoch_pushed_down_txn_num_local_vec;

    void MOT::Init() {
        thread_id = inc_id.fetch_add(1);
        sharding_num = TaasContext::kTxnNodeNum;
        max_length = TaasContext::kCacheMaxLength;
        local_server_id = TaasContext::txn_node_ip_index;
        epoch_should_push_down_txn_num_local= std::make_shared<AtomicCounters_Cache>(max_length, sharding_num);
        epoch_pushed_down_txn_num_local= std::make_shared<AtomicCounters_Cache>(max_length, sharding_num);
        epoch_should_push_down_txn_num_local_vec[thread_id] = epoch_should_push_down_txn_num_local;
        epoch_pushed_down_txn_num_local_vec[thread_id] = epoch_pushed_down_txn_num_local;
    }

    void MOT::StaticInit() {
        task_queue = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
        redo_log_queue = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
        epoch_redo_log_complete.resize(TaasContext::kCacheMaxLength);
        epoch_redo_log_queue.resize(TaasContext::kCacheMaxLength);
        for(int i = 0; i < static_cast<int>(TaasContext::kCacheMaxLength); i ++) {
            epoch_redo_log_complete[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_redo_log_queue[i] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
        }
        epoch_should_push_down_txn_num_local_vec.resize(StorageContext::kMOTThreadNum);
        epoch_pushed_down_txn_num_local_vec.resize(StorageContext::kMOTThreadNum);
    }

    void MOT::StaticClear(const uint64_t &epoch) {
        epoch_redo_log_complete[epoch % TaasContext::kCacheMaxLength]->store(false);
//        epoch_redo_log_queue[epoch % TaasContext::kCacheMaxLength] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
        ClearAllThreadLocalCountNum(epoch, epoch_should_push_down_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, epoch_pushed_down_txn_num_local_vec);
    }

    void MOT::ClearAllThreadLocalCountNum(const uint64_t &epoch, const std::vector<std::shared_ptr<AtomicCounters_Cache>> &vec) {
        for(const auto& i : vec) {
            if(i != nullptr)
                i->Clear(epoch);
        }
    }
    uint64_t MOT::GetAllThreadLocalCountNum(const uint64_t &epoch, const std::vector<std::shared_ptr<AtomicCounters_Cache>> &vec) {
        uint64_t ans = 0;
        for(const auto& i : vec) {
            if(i != nullptr)
                ans += i->GetCount(epoch);
        }
        return ans;
    }
    uint64_t MOT::GetAllThreadLocalCountNum(const uint64_t &epoch, const uint64_t &sharding_id, const std::vector<std::shared_ptr<AtomicCounters_Cache>> &vec) {
        uint64_t ans = 0;
        for(const auto& i : vec) {
            if(i != nullptr)
                ans += i->GetCount(epoch, sharding_id);
        }
        return ans;
    }

    bool MOT::CheckEpochPushDownComplete(const uint64_t &epoch) {
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
    void MOT::DBRedoLogQueueEnqueue(const uint64_t& thread_id, const uint64_t &epoch, std::shared_ptr<proto::Transaction> txn_ptr) {
        epoch_should_push_down_txn_num_local_vec[thread_id % inc_id.load() ]->IncCount(epoch, txn_ptr->txn_server_id(), 1);
        auto epoch_mod = epoch % TaasContext::kCacheMaxLength;
        epoch_redo_log_queue[epoch_mod]->enqueue(txn_ptr);
        epoch_redo_log_queue[epoch_mod]->enqueue(nullptr);
        txn_ptr.reset();
    }

    bool MOT::DBRedoLogQueueTryDequeue(const uint64_t &epoch, std::shared_ptr<proto::Transaction> txn_ptr) {
        auto epoch_mod = epoch % TaasContext::kCacheMaxLength;
        return epoch_redo_log_queue[epoch_mod]->try_dequeue(txn_ptr);
    }

    bool MOT::GeneratePushDownTask(const uint64_t &epoch) {
        auto txn_ptr = std::make_shared<proto::Transaction>();
        txn_ptr->set_commit_epoch(epoch);
        task_queue->enqueue(txn_ptr);
        task_queue->enqueue(nullptr);
        return true;
    }

    void MOT::SendTransactionToDB_Usleep() {
        bool sleep_flag;
        std::shared_ptr<proto::Transaction> txn_ptr;
        uint64_t epoch, epoch_mod;
        proto::Transaction* ptr;
        epoch = EpochManager::GetPushDownEpoch();
        sleep_flag = true;
        while (!EpochManager::IsTimerStop()) {
            epoch = EpochManager::GetPushDownEpoch();
            while(!EpochManager::IsRecordCommitted(epoch)) {
                usleep(storage_sleep_time);
                epoch = EpochManager::GetPushDownEpoch();
            }
            epoch_mod = epoch % TaasContext::kCacheMaxLength;
            sleep_flag = true;
//            uint64_t cnt = 0;
            while(epoch_redo_log_queue[epoch_mod]->try_dequeue(txn_ptr)) {
//                cnt ++;
//                LOG(INFO) << "Try Dequeue MOT, epoch : " <<  epoch_mod << " cnt: " << cnt;
                if(txn_ptr == nullptr || txn_ptr->txn_type() == proto::TxnType::NullMark) {
                    continue;
                }
//                commit_cv.notify_all();
//                LOG(INFO) << "Send a txn to MOT, epoch : " <<  txn_ptr->commit_epoch();
                epoch = txn_ptr->commit_epoch();
                auto push_msg = std::make_unique<proto::Message>();
                auto push_response = push_msg->mutable_storage_push_response();
                push_response->set_result(proto::Success);
                push_response->set_epoch_id(epoch);
                push_response->set_txn_num(1);
                ptr = push_response->add_txns();
                *ptr = *txn_ptr;
                /// *(ptr) = (*txn_ptr);
                auto serialized_pull_resp_str = std::make_unique<std::string>();
                Gzip(push_msg.get(), serialized_pull_resp_str.get());
                MessageQueue::send_to_mot_storage_queue->enqueue(std::make_unique<send_params>(0, 0,
                       "", epoch, proto::TxnType::CommittedTxn, std::move(serialized_pull_resp_str), nullptr));
                epoch_pushed_down_txn_num_local->IncCount(epoch, epoch, 1);
                txn_ptr.reset();
                sleep_flag = false;
            }
            if(sleep_flag)
                usleep(sleep_time);
        }
//        if(sleep_flag)
//            usleep(storage_sleep_time);
    }

    void MOT::SendTransactionToDB_Block() {
        proto::Transaction* ptr;
        std::mutex mtx;
        std::unique_lock lck(mtx);
        uint64_t epoch, epoch_mod;
        bool sleep_flag;
        std::shared_ptr<proto::Transaction> txn_ptr;
        while(!EpochManager::IsTimerStop()) {
            epoch = EpochManager::GetPushDownEpoch();
            while(!EpochManager::IsCommitComplete(epoch)) {
                commit_cv.wait(lck);
                epoch = EpochManager::GetPushDownEpoch();
            }
            epoch_mod = epoch % TaasContext::kCacheMaxLength;
            sleep_flag = true;
            while(epoch_redo_log_queue[epoch_mod]->try_dequeue(txn_ptr)) {
                if(txn_ptr == nullptr || txn_ptr->txn_type() == proto::TxnType::NullMark) {
                    continue;
                }
                auto push_msg = std::make_unique<proto::Message>();
                auto push_response = push_msg->mutable_storage_push_response();
                push_response->set_result(proto::Success);
                push_response->set_epoch_id(epoch);
                push_response->set_txn_num(1);
                ptr = push_response->add_txns();
                *ptr = *txn_ptr;
                /// *(ptr) = (*txn_ptr);
                auto serialized_pull_resp_str = std::make_unique<std::string>();
                Gzip(push_msg.get(), serialized_pull_resp_str.get());
                MessageQueue::send_to_mot_storage_queue->enqueue(std::make_unique<send_params>(0, 0,"", epoch, proto::TxnType::CommittedTxn, std::move(serialized_pull_resp_str), nullptr));
                epoch_pushed_down_txn_num_local->IncCount(epoch, epoch, 1);
                txn_ptr.reset();
                sleep_flag = false;
            }
//            if(sleep_flag)
//                usleep(storage_sleep_time);
            if(sleep_flag)
                usleep(sleep_time);
        }
    }

//    void MOT::SendToMOThreadMain_usleep() {
////        int queue_length = 0;
////        zmq::context_t context(1);
////        zmq::message_t reply(5);
////        zmq::send_flags sendFlags = zmq::send_flags::none;
////        zmq::socket_t socket_send(context, ZMQ_PUB);
////        socket_send.set(zmq::sockopt::sndhwm, queue_length);
////        socket_send.set(zmq::sockopt::rcvhwm, queue_length);
////        socket_send.bind("tcp://*:5556");//to server
////        printf("线程开始工作 SendStorage PUBServerThread ZMQ_PUB tcp://*:5556\n");
////        std::unique_ptr<send_params> params;
////        std::unique_ptr<zmq::message_t> msg;
////        std::shared_ptr<proto::Transaction> txn_ptr;
////        uint64_t epoch = 1;
//        while(!EpochManager::IsInitOK()) usleep(sleep_time);
//        while (!EpochManager::IsTimerStop()) {
//            if(epoch < EpochManager::GetLogicalEpoch()) {
//                auto push_msg = std::make_unique<proto::Message>();
//                auto push_response = push_msg->mutable_storage_push_response();
//                auto s = std::to_string(epoch) + ":";
//                auto epoch_mod = epoch % EpochManager::max_length;
//                auto total_num = RedoLoger::epoch_log_lsn.GetCount(epoch);
//                for (uint64_t i = 0; i < total_num; i++) {
//                    auto key = s + std::to_string(i);
//                    auto *ptr = push_response->add_txns();
//                    RedoLoger::committed_txn_cache[epoch_mod]->getValue(key, (*ptr)); //copy
//                }
//                push_response->set_result(proto::Success);
//                push_response->set_epoch_id(epoch);
//                push_response->set_txn_num(total_num);
//                auto serialized_pull_resp_str = std::make_unique<std::string>();
//                Gzip(push_msg.get(), serialized_pull_resp_str.get());
//                auto send_message = std::make_unique<zmq::message_t>(*serialized_pull_resp_str);
//                socket_send.send((*send_message), sendFlags);
//                MOT::IncPushedDownMOTEpoch();
//                epoch ++;
//            }
//            else {
//                CheckRedoLogPushDownState(ctx);
//                usleep(sleep_time);
//            }
//        }
//        socket_send.send((zmq::message_t &) "end", sendFlags);
//    }
//
//    void MOT::SendToMOThreadMain() {//PUB PACK
//        int queue_length = 0;
//        zmq::context_t context(1);
//        zmq::message_t reply(5);
//        zmq::send_flags sendFlags = zmq::send_flags::none;
//        zmq::socket_t socket_send(context, ZMQ_PUB);
//        socket_send.set(zmq::sockopt::sndhwm, queue_length);
//        socket_send.set(zmq::sockopt::rcvhwm, queue_length);
//        socket_send.bind("tcp://*:5556");//to server
//        printf("线程开始工作 SendStorage PUBServerThread ZMQ_PUB tcp://*:5556\n");
//        std::unique_ptr<send_params> params;
//        std::unique_ptr<zmq::message_t> msg;
//        std::shared_ptr<proto::Transaction> txn_ptr;
//        uint64_t epoch;
//        while(!EpochManager::IsInitOK()) usleep(sleep_time);
//        while (!EpochManager::IsTimerStop()) {
//            MOT::task_queue->wait_dequeue(txn_ptr);
//            if(txn_ptr != nullptr) {
//                epoch = txn_ptr->commit_epoch();
//                auto push_msg = std::make_unique<proto::Message>();
//                auto push_response = push_msg->mutable_storage_push_response();
//                auto s = std::to_string(epoch) + ":";
//                auto epoch_mod = epoch % EpochManager::max_length;
//                auto total_num = RedoLoger::epoch_log_lsn.GetCount(epoch);
//                for (uint64_t i = 0; i < total_num; i++) {
//                    auto key = s + std::to_string(i);
//                    auto *ptr = push_response->add_txns();
//                    RedoLoger::committed_txn_cache[epoch_mod]->getValue(key, (*ptr)); //copy
//                }
//                push_response->set_result(proto::Success);
//                push_response->set_epoch_id(epoch);
//                push_response->set_txn_num(total_num);
//                auto serialized_pull_resp_str = std::make_unique<std::string>();
//                Gzip(push_msg.get(), serialized_pull_resp_str.get());
//                auto send_message = std::make_unique<zmq::message_t>(*serialized_pull_resp_str);
//                socket_send.send((*send_message), sendFlags);
//                MOT::IncPushedDownMOTEpoch();
//            }
//        }
//        socket_send.send((zmq::message_t &) "end", sendFlags);
//    }

}