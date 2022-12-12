//
// Created by 周慰星 on 11/9/22.
//

#ifndef TAAS_HANDLER_H
#define TAAS_HANDLER_H

#include "epoch/epoch_manager.h"
#include "utils/utilities.h"

namespace Taas {
    class MessageHandler{
    public:
        bool Init(uint64_t id, Context context);
        bool HandleReceiveMessage();
        bool HandleLocalMergedTxn();
        bool CheckTxnReceiveComplete() const;
        bool HandleTxnCachea();
    private:
        std::unique_ptr<zmq::message_t> message_ptr;
        std::unique_ptr<std::string> message_string_ptr;
        std::unique_ptr<proto::Message> msg_ptr;
        std::unique_ptr<proto::Transaction> txn_ptr;
        std::unique_ptr<pack_params> pack_param;
        std::string csn_temp, key_temp, key_str, table_name, csn_result;
        uint64_t thread_id = 0, server_dequeue_id = 0, epoch_mod = 0,
                epoch = 0, clear_epoch = 0,max_length = 0;
        bool res, sleep_flag;
        std::vector<std::vector<std::unique_ptr<std::queue<std::unique_ptr<proto::Transaction>>>>> message_cache;
        Context ctx;
    };

}

#endif //TAAS_HANDLER_H
