//
// Created by 周慰星 on 23-3-30.
//

#ifndef TAAS_MESSAGE_H
#define TAAS_MESSAGE_H

#pragma once

#include "tools/atomic_counters.h"
#include "tools/blocking_concurrent_queue.hpp"
#include "tools/blocking_mpmc_queue.h"
#include "tools/concurrent_hash_map.h"
#include "tools/context.h"

#include "zmq.hpp"
#include "tikv_client.h"
#include "proto/message.pb.h"

namespace Taas {

    struct pack_params {
        uint64_t id{};/// send to whom
        uint64_t time{};
        std::string ip; /// send to whom
        uint64_t epoch{};
        proto::TxnType type{};
        std::unique_ptr<std::string> str;
        std::shared_ptr<proto::Transaction> txn;
        explicit pack_params(uint64_t id_, uint64_t time_, std::string ip_, uint64_t e = 0, proto::TxnType ty = proto::TxnType::NullMark,
                             std::unique_ptr<std::string> && s = nullptr, std::shared_ptr<proto::Transaction> &&t = nullptr):
                id(id_), time(time_), ip(std::move(ip_)), epoch(e), type(ty), str(std::move(s)), txn(std::move(t)){}
        pack_params()= default;
    };

    struct send_params {
        uint64_t id{}; /// send to whom
        uint64_t time{};
        std::string ip; /// send to whom
        uint64_t epoch{};
        proto::TxnType type{};
        std::unique_ptr<std::string> str;
        std::shared_ptr<proto::Transaction> txn;
        bool send_to_all;
//        send_params(uint64_t id_, uint64_t time_, std::string ip_, uint64_t e = 0, proto::TxnType ty = proto::TxnType::NullMark,
//                    std::unique_ptr<std::string> && s = nullptr, std::shared_ptr<proto::Transaction> &&t = nullptr):
//                id(id_), time(time_), ip(std::move(ip_)), epoch(e), type(ty), str(std::move(s)), txn(std::move(t)), send_to_all(false){}
        send_params(uint64_t id_, uint64_t time_, std::string ip_, uint64_t e = 0, proto::TxnType ty = proto::TxnType::NullMark,
                    std::unique_ptr<std::string> && s = nullptr, std::shared_ptr<proto::Transaction> &&t = nullptr, bool send_to_all_t = false):
                id(id_), time(time_), ip(std::move(ip_)), epoch(e), type(ty), str(std::move(s)), txn(std::move(t)), send_to_all(send_to_all_t){}
        send_params()= default;
    };

    class MessageQueue{
    public:
        static std::unique_ptr<MessageBlockingConcurrentQueue<std::unique_ptr<zmq::message_t>>> listen_message_queue, listen_message_txn_queue, listen_message_epoch_queue;
        static std::unique_ptr<MessageBlockingConcurrentQueue<std::unique_ptr<send_params>>> send_to_server_queue, send_to_client_queue,
                send_to_storage_queue, send_to_mot_storage_queue, send_to_nebula_storage_queue, send_to_server_pub_queue;
        static std::unique_ptr<MessageBlockingConcurrentQueue<std::unique_ptr<proto::Message>>> request_queue, raft_message_queue;
        static void StaticInitMessageQueue();
        static std::atomic<uint64_t> client_receive_message_num, client_send_message_num;
    };

    //message transport threads
    extern void SendServerThreadMain();
    extern void SendServerPUBThreadMain();
    extern void ListenServerThreadMain();
    extern void ListenServerThreadMain_Sub();
    extern void SendClientThreadMain();
    extern void ListenClientThreadMain();
    extern void ListenStorageThreadMain();
    extern void SendToMOTStorageThreadMain();
    extern void SendToNebulaStorageThreadMain();

}

#endif //TAAS_MESSAGE_H
