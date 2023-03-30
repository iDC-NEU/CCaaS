//
// Created by 周慰星 on 23-3-30.
//

#ifndef TAAS_MESSAGE_H
#define TAAS_MESSAGE_H
#include "proto/message.pb.h"
#include "tikv_client.h"
#include "tools/atomic_counters.h"
#include "tools/blocking_concurrent_queue.hpp"
#include "tools/concurrent_hash_map.h"
#include "tools/context.h"
#include "zmq.hpp"

namespace Taas {
    struct pack_params {
        uint64_t id{};/// send to whom
        uint64_t time{};
        std::string ip; /// send to whom
        uint64_t epoch{};
        proto::TxnType type{};
        std::unique_ptr<std::string> str;
        std::unique_ptr<proto::Transaction> txn;
        explicit pack_params(uint64_t id_, uint64_t time_, std::string ip_, uint64_t e = 0, proto::TxnType ty = proto::TxnType::NullMark,
                             std::unique_ptr<std::string> && s = nullptr, std::unique_ptr<proto::Transaction> &&t = nullptr):
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
        std::unique_ptr<proto::Transaction> txn;
        send_params(uint64_t id_, uint64_t time_, std::string ip_, uint64_t e = 0, proto::TxnType ty = proto::TxnType::NullMark,
                    std::unique_ptr<std::string> && s = nullptr, std::unique_ptr<proto::Transaction> &&t = nullptr):
                id(id_), time(time_), ip(std::move(ip_)), epoch(e), type(ty), str(std::move(s)), txn(std::move(t)){}
        send_params()= default;
    };

    template<typename T>
    using  BlockingConcurrentQueue = moodycamel::BlockingConcurrentQueue<T>;
    ///message transmit
    extern std::unique_ptr<BlockingConcurrentQueue<std::unique_ptr<zmq::message_t>>> listen_message_queue;
    extern std::unique_ptr<BlockingConcurrentQueue<std::unique_ptr<send_params>>> send_to_server_queue, send_to_client_queue, send_to_storage_queue;
    extern std::unique_ptr<BlockingConcurrentQueue<std::unique_ptr<proto::Message>>> request_queue, raft_message_queue;

    extern void InitMessage(Context& ctx);
    //message transport threads
    extern void SendServerThreadMain(Context ctx);
    extern void ListenServerThreadMain(const Context& ctx);
    extern void SendClientThreadMain(Context ctx);
    extern void ListenClientThreadMain(Context ctx);
    extern void ListenStorageThreadMain(Context ctx);
    extern void SendStoragePUBThreadMain(Context ctx);
    extern void SendStoragePUBThreadMain2(Context ctx);


}

#endif //TAAS_MESSAGE_H
