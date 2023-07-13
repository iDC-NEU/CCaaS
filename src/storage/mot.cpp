//
// Created by zwx on 23-4-9.
//
#include "storage/mot.h"
#include "tools/utilities.h"
#include "message/message.h"
#include "epoch/epoch_manager.h"

namespace Taas {

    std::atomic<uint64_t> MOT::pushed_down_mot_epoch;
    Context MOT::ctx;
    AtomicCounters_Cache
            MOT::epoch_should_push_down_txn_num(10, 1), MOT::epoch_pushed_down_txn_num(10, 1);
    std::unique_ptr<moodycamel::BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>  MOT::task_queue, MOT::redo_log_queue;
    std::vector<std::unique_ptr<moodycamel::BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>>
            MOT::epoch_redo_log_queue;
    std::vector<std::unique_ptr<std::atomic<bool>>> MOT::epoch_redo_log_complete;

    bool MOT::StaticInit(const Context &ctx_) {
        ctx = ctx_;
        MOT::pushed_down_mot_epoch.store(1);
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
        return true;
    }

    bool MOT::GeneratePushDownTask(uint64_t &epoch) {
        auto txn_ptr = std::make_unique<proto::Transaction>();
        txn_ptr->set_commit_epoch(epoch);
        task_queue->enqueue(std::move(txn_ptr));
        task_queue->enqueue(nullptr);
        return true;
    }

    void MOT::SendTransactionToDB_Usleep() {
        bool sleep_flag;
        std::unique_ptr<proto::Transaction> txn_ptr;
        uint64_t epoch;
        epoch = EpochManager::GetPushDownEpoch();
        auto epoch_mod = epoch % ctx.kCacheMaxLength;
        sleep_flag = true;
        while (!EpochManager::IsTimerStop()) {
            epoch = EpochManager::GetLogicalEpoch();
            while(epoch_redo_log_queue[epoch_mod]->try_dequeue(txn_ptr)) {
                if(txn_ptr == nullptr || txn_ptr->txn_type() == proto::TxnType::NullMark) {
                    continue;
                }
                redo_log_queue->enqueue(std::move(txn_ptr));
                redo_log_queue->enqueue(nullptr);
            }

            while(redo_log_queue->try_dequeue(txn_ptr)) {
                if (txn_ptr == nullptr || txn_ptr->txn_type() == proto::TxnType::NullMark) {
                    return;
                }
                epoch = txn_ptr->commit_epoch();
                auto push_msg = std::make_unique<proto::Message>();
                auto push_response = push_msg->mutable_storage_push_response();
                push_response->set_result(proto::Success);
                push_response->set_epoch_id(epoch);
                push_response->set_txn_num(1);
                auto *ptr = push_response->add_txns();
                ptr = txn_ptr.release();
                auto serialized_pull_resp_str = std::make_unique<std::string>();
                Gzip(push_msg.get(), serialized_pull_resp_str.get());
                MessageQueue::send_to_storage_queue->enqueue(std::make_unique<send_params>(0, 0,
                       "", epoch, proto::TxnType::CommittedTxn, std::move(serialized_pull_resp_str), nullptr));
                epoch_pushed_down_txn_num.IncCount(txn_ptr->commit_epoch(), txn_ptr->server_id(), 1);
                sleep_flag = false;
            }
        }
        if(sleep_flag)
            usleep(sleep_time);
    }

    void MOT::SendTransactionToDB_Block() {
        std::unique_ptr<proto::Transaction> txn_ptr;
        uint64_t epoch;
        epoch = EpochManager::GetPushDownEpoch();
        while (!EpochManager::IsTimerStop()) {
            redo_log_queue->wait_dequeue(txn_ptr);
            if (txn_ptr == nullptr || txn_ptr->txn_type() == proto::TxnType::NullMark) {
                return;
            }
            epoch = txn_ptr->commit_epoch();
            auto push_msg = std::make_unique<proto::Message>();
            auto push_response = push_msg->mutable_storage_push_response();
            push_response->set_result(proto::Success);
            push_response->set_epoch_id(epoch);
            push_response->set_txn_num(1);
            auto *ptr = push_response->add_txns();
            ptr = txn_ptr.release();
            auto serialized_pull_resp_str = std::make_unique<std::string>();
            Gzip(push_msg.get(), serialized_pull_resp_str.get());
            MessageQueue::send_to_storage_queue->enqueue(std::make_unique<send_params>(0, 0,"", epoch,proto::TxnType::CommittedTxn,std::move(serialized_pull_resp_str),nullptr));
            epoch_pushed_down_txn_num.IncCount(txn_ptr->commit_epoch(), txn_ptr->server_id(), 1);
        }
    }

    bool MOT::CheckEpochPushDownComplete(uint64_t &epoch) {
        if(epoch_redo_log_complete[epoch % ctx.kCacheMaxLength]->load()) return true;
        if(epoch < EpochManager::GetLogicalEpoch() &&
           epoch_pushed_down_txn_num.GetCount(epoch) >= epoch_should_push_down_txn_num.GetCount(epoch)) {
            epoch_redo_log_complete[epoch % ctx.kCacheMaxLength]->store(true);
            pushed_down_mot_epoch.fetch_add(1);
            return true;
        }
        return false;
    }

    void MOT::DBRedoLogQueueEnqueue(uint64_t &epoch, std::unique_ptr<proto::Transaction> &&txn_ptr) {
        auto epoch_mod = epoch % ctx.kCacheMaxLength;
        epoch_redo_log_queue[epoch_mod]->enqueue(std::move(txn_ptr));
        epoch_redo_log_queue[epoch_mod]->enqueue(nullptr);
    }

    bool MOT::DBRedoLogQueueTryDequeue(uint64_t &epoch, std::unique_ptr<proto::Transaction> &txn_ptr) {
        auto epoch_mod = epoch % ctx.kCacheMaxLength;
        return epoch_redo_log_queue[epoch_mod]->try_dequeue(txn_ptr);
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
////        std::unique_ptr<proto::Transaction> txn_ptr;
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
//        std::unique_ptr<proto::Transaction> txn_ptr;
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