//
// Created by user on 23-7-16.
//

#ifndef TAAS_TWO_PHASE_COMMIT_H
#define TAAS_TWO_PHASE_COMMIT_H
#pragma once

#include "epoch/epoch_manager.h"
#include "tools/utilities.h"
#include "tools/concurrent_hash_map.h"
#include "message/twoPC_message_receive_handler.h"

#include <zmq.hpp>

namespace Taas {
    class TwoPC {
    public:
        static concurrent_unordered_map<std::string, std::unique_ptr<uint64_t>> row_lock_map;   /// key, tid
        static concurrent_unordered_map<std::string, std::unique_ptr<TwoPCTxnStateStruct>> txn_state_map;   /// tid, txn struct

        static bool Init(const Taas::Context &ctx_, uint64_t id);
        static bool HandleReceivedMessage();
        static bool HandleReceivedTxn();
        static bool SetMessageRelatedCountersInfo();
    };
}

#endif //TAAS_TWO_PHASE_COMMIT_H
