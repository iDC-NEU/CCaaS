//
// Created by user on 23-4-9.
//
#include "storage/mot.h"
#include "zmq.hpp"
#include "tools/utilities.h"
#include "message/message.h"
#include "epoch/epoch_manager.h"
#include "storage/redo_loger.h"

namespace Taas {

    Context MOT::ctx;
    std::atomic<uint64_t> MOT::pushed_down_mot_epoch;
    std::unique_ptr<moodycamel::BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>  MOT::task_queue;

    bool MOT::StaticInit(const Context &ctx_) {
        ctx = ctx_;
        MOT::pushed_down_mot_epoch.store(1);
        MOT::task_queue = std::make_unique<moodycamel::BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>();
        return true;
    }

    bool MOT::GeneratePushDownTask(uint64_t &epoch) {
        auto txn_ptr = std::make_unique<proto::Transaction>();
        txn_ptr->set_commit_epoch(epoch);
        task_queue->enqueue(std::move(txn_ptr));
        task_queue->enqueue(nullptr);
        return true;
    }

    void MOT::SendToMOThreadMain_usleep() {
        SetCPU();
        int queue_length = 0;
        zmq::context_t context(1);
        zmq::message_t reply(5);
        zmq::send_flags sendFlags = zmq::send_flags::none;
        zmq::socket_t socket_send(context, ZMQ_PUB);
        socket_send.set(zmq::sockopt::sndhwm, queue_length);
        socket_send.set(zmq::sockopt::rcvhwm, queue_length);
        socket_send.bind("tcp://*:5556");//to server
        printf("线程开始工作 SendStorage PUBServerThread ZMQ_PUB tcp://*:5556\n");
        std::unique_ptr<send_params> params;
        std::unique_ptr<zmq::message_t> msg;
        std::unique_ptr<proto::Transaction> txn_ptr;
        uint64_t epoch = 1;
        while(!EpochManager::IsInitOK()) usleep(1000);
        while (!EpochManager::IsTimerStop()) {
            if(epoch < EpochManager::GetLogicalEpoch()) {
                auto push_msg = std::make_unique<proto::Message>();
                auto push_response = push_msg->mutable_storage_push_response();
                auto s = std::to_string(epoch) + ":";
                auto epoch_mod = epoch % EpochManager::max_length;
                auto total_num = RedoLoger::epoch_log_lsn.GetCount(epoch);
                for (uint64_t i = 0; i < total_num; i++) {
                    auto key = s + std::to_string(i);
                    auto *ptr = push_response->add_txns();
                    RedoLoger::committed_txn_cache[epoch_mod]->getValue(key, (*ptr)); //copy
                }
                push_response->set_result(proto::Success);
                push_response->set_epoch_id(epoch);
                push_response->set_txn_num(total_num);
                auto serialized_pull_resp_str = std::make_unique<std::string>();
                Gzip(push_msg.get(), serialized_pull_resp_str.get());
                auto send_message = std::make_unique<zmq::message_t>(*serialized_pull_resp_str);
                socket_send.send((*send_message), sendFlags);
                MOT::IncPushedDownMOTEpoch();
                epoch ++;
            }
            else {
                usleep(50);
            }
            EpochManager::CheckRedoLogPushDownState();
        }
        socket_send.send((zmq::message_t &) "end", sendFlags);
    }

    void MOT::SendToMOThreadMain() {//PUB PACK
        SetCPU();
        int queue_length = 0;
        zmq::context_t context(1);
        zmq::message_t reply(5);
        zmq::send_flags sendFlags = zmq::send_flags::none;
        zmq::socket_t socket_send(context, ZMQ_PUB);
        socket_send.set(zmq::sockopt::sndhwm, queue_length);
        socket_send.set(zmq::sockopt::rcvhwm, queue_length);
        socket_send.bind("tcp://*:5556");//to server
        printf("线程开始工作 SendStorage PUBServerThread ZMQ_PUB tcp://*:5556\n");
        std::unique_ptr<send_params> params;
        std::unique_ptr<zmq::message_t> msg;
        std::unique_ptr<proto::Transaction> txn_ptr;
        uint64_t epoch;
        while(!EpochManager::IsInitOK()) usleep(1000);
        while (!EpochManager::IsTimerStop()) {
            MOT::task_queue->wait_dequeue(txn_ptr);
            if(txn_ptr != nullptr) {
                epoch = txn_ptr->commit_epoch();
                auto push_msg = std::make_unique<proto::Message>();
                auto push_response = push_msg->mutable_storage_push_response();
                auto s = std::to_string(epoch) + ":";
                auto epoch_mod = epoch % EpochManager::max_length;
                auto total_num = RedoLoger::epoch_log_lsn.GetCount(epoch);
                for (uint64_t i = 0; i < total_num; i++) {
                    auto key = s + std::to_string(i);
                    auto *ptr = push_response->add_txns();
                    RedoLoger::committed_txn_cache[epoch_mod]->getValue(key, (*ptr)); //copy
                }
                push_response->set_result(proto::Success);
                push_response->set_epoch_id(epoch);
                push_response->set_txn_num(total_num);
                auto serialized_pull_resp_str = std::make_unique<std::string>();
                Gzip(push_msg.get(), serialized_pull_resp_str.get());
                auto send_message = std::make_unique<zmq::message_t>(*serialized_pull_resp_str);
                socket_send.send((*send_message), sendFlags);
                MOT::IncPushedDownMOTEpoch();
            }
        }
        socket_send.send((zmq::message_t &) "end", sendFlags);
    }

}