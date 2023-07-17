//
// Created by user on 23-7-16.
//

#ifndef TAAS_TWO_PHASE_COMMIT_H
#define TAAS_TWO_PHASE_COMMIT_H

#include "message/message.h"

#include <zmq.hpp>

namespace Taas {
    class TwoPC {
    public:
        bool HandleReceivedMessage();
        bool HandleReceivedTxn();
        bool Init(const Context& ctx_, uint64_t id);
    private:
        std::unique_ptr<zmq::message_t> message_ptr;
        std::unique_ptr<std::string> message_string_ptr;
        std::unique_ptr<proto::Message> msg_ptr;
        std::unique_ptr<proto::Transaction> txn_ptr;
        std::unique_ptr<pack_params> pack_param;
        std::string csn_temp, key_temp, key_str, table_name, csn_result;
        uint64_t thread_id = 0,
                server_dequeue_id = 0, epoch_mod = 0, epoch = 0, max_length = 0,sharding_num = 0,///cache check
        message_epoch = 0, message_epoch_mod = 0, message_sharding_id = 0, message_server_id = 0, ///message epoch info
        server_reply_ack_id = 0,
                cache_clear_epoch_num = 0, cache_clear_epoch_num_mod = 0,
                redo_log_push_down_reply = 1;

        bool res, sleep_flag;
        Context ctx;
        proto::Transaction empty_txn;
        std::hash<std::string> _hash;

        bool SetMessageRelatedCountersInfo();
    };
}

#endif //TAAS_TWO_PHASE_COMMIT_H
