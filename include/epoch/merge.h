//
// Created by 周慰星 on 11/15/22.
//

#ifndef TAAS_MERGE_H
#define TAAS_MERGE_H

#include <cstdint>
#include "zmq.hpp"
#include "proto/message.pb.h"
#include "transaction/crdt_merge.h"
#include "message/handler_send.h"
#include "message/handler_receive.h"

namespace Taas {

    class Merger {

        std::unique_ptr<zmq::message_t> message_ptr;
        std::unique_ptr<std::string> message_string_ptr;
        std::unique_ptr<proto::Message> msg_ptr;
        std::unique_ptr<proto::Transaction> txn_ptr;
        std::unique_ptr<pack_params> pack_param;
        std::string csn_temp, key_temp, key_str, table_name, csn_result;
        uint64_t thread_id = 0, epoch = 0;
        bool res, sleep_flag;
        Context ctx;
        CRDTMerge merger;
        MessageSendHandler message_transmitter;
        MessageReceiveHandler message_handler;


    public:
        void Init(uint64_t id, Context ctx);
        bool EpochMerge();
        bool EpochCommit_RedoLog_TxnMode();
        bool EpochCommit_RedoLog_ShardingMode();
        bool Run(uint64_t id, Context ctx);
    };
}

#endif //TAAS_MERGE_H
