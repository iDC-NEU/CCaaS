//
// Created by 周慰星 on 23-3-30.
//

#include "message/message.h"
#include "proto/message.pb.h"
namespace Taas {
    // 接受client和peer txn node发来的写集，都放在listen_message_queue中
    std::unique_ptr<MessageBlockingConcurrentQueue<std::unique_ptr<zmq::message_t>>>
            MessageQueue::listen_message_queue, MessageQueue::listen_message_txn_queue, MessageQueue::listen_message_epoch_queue;
//    std::unique_ptr<MessageBlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>> MessageQueue::listen_message_txn_queue, MessageQueue::listen_message_epoch_queue;
    std::unique_ptr<MessageBlockingConcurrentQueue<std::unique_ptr<send_params>>> MessageQueue::send_to_server_queue,
            MessageQueue::send_to_client_queue, MessageQueue::send_to_storage_queue, MessageQueue::send_to_mot_storage_queue,
            MessageQueue::send_to_nebula_storage_queue, MessageQueue::send_to_server_pub_queue;
    std::unique_ptr<MessageBlockingConcurrentQueue<std::unique_ptr<proto::Message>>> MessageQueue::request_queue,
            MessageQueue::raft_message_queue;

    void MessageQueue::StaticInitMessageQueue(const Context& ctx) {
        listen_message_queue = std::make_unique<MessageBlockingConcurrentQueue<std::unique_ptr<zmq::message_t>>>();
        listen_message_txn_queue = std::make_unique<MessageBlockingConcurrentQueue<std::unique_ptr<zmq::message_t>>>();
        listen_message_epoch_queue = std::make_unique<MessageBlockingConcurrentQueue<std::unique_ptr<zmq::message_t>>>();
        send_to_server_queue = std::make_unique<MessageBlockingConcurrentQueue<std::unique_ptr<send_params>>>();
        send_to_server_pub_queue = std::make_unique<MessageBlockingConcurrentQueue<std::unique_ptr<send_params>>>();
        send_to_client_queue = std::make_unique<MessageBlockingConcurrentQueue<std::unique_ptr<send_params>>>();
        send_to_storage_queue = std::make_unique<MessageBlockingConcurrentQueue<std::unique_ptr<send_params>>>();
        send_to_mot_storage_queue = std::make_unique<MessageBlockingConcurrentQueue<std::unique_ptr<send_params>>>();
        send_to_nebula_storage_queue = std::make_unique<MessageBlockingConcurrentQueue<std::unique_ptr<send_params>>>();
        request_queue = std::make_unique<MessageBlockingConcurrentQueue<std::unique_ptr<proto::Message>>>();
        raft_message_queue = std::make_unique<MessageBlockingConcurrentQueue<std::unique_ptr<proto::Message>>>();
    }

}
