//
// Created by 周慰星 on 11/9/22.
//

#ifndef TAAS_EPOCH_MESSAGE_RECEIVE_HANDLER_H
#define TAAS_EPOCH_MESSAGE_RECEIVE_HANDLER_H

#pragma once

#include "queue"

#include "zmq.hpp"

#include "message/message.h"
#include "tools/utilities.h"
#include "tools/thread_pool_light.h"
#include "tools/thread_counters.h"

namespace Taas {

class EpochMessageReceiveHandler : public ThreadCounters {
    public:
        bool Init(const uint64_t &id);

        void HandleReceivedMessage();
        void TryHandleReceivedMessage();
        void HandleReceivedControlMessage();
        void TryHandleReceivedControlMessage();
        bool SetMessageRelatedCountersInfo();
        bool HandleReceivedTxn();
        void HandleMultiModelClientSubTxn(const uint64_t& txn_id);
        uint64_t getMultiModelTxnId();
        bool HandleMultiModelClientTxn();
        bool UpdateEpochAbortSet();


        [[nodiscard]] uint64_t GetHashValue(const std::string& key) const {
            return _hash(key) % shard_num;
        }

        void ReadValidateQueueEnqueue(uint64_t &epoch_, const std::shared_ptr<proto::Transaction> &txn_ptr_);
        void MergeQueueEnqueue(uint64_t &epoch_, const std::shared_ptr<proto::Transaction>& txn_ptr_);
        void CommitQueueEnqueue(uint64_t &epoch_, const std::shared_ptr<proto::Transaction>& txn_ptr_);
        void RedoLogQueueEnqueue(uint64_t &epoch_, const std::shared_ptr<proto::Transaction>& txn_ptr_);
        void ResultReturnQueueEnqueue(uint64_t &epoch_, const std::shared_ptr<proto::Transaction>& txn_ptr_);

        static bool StaticInit();
        static bool StaticClear([[maybe_unused]] uint64_t& epoch);

    private:
        std::unique_ptr<zmq::message_t> message_ptr;
        std::unique_ptr<std::string> message_string_ptr;
        std::unique_ptr<proto::Message> msg_ptr;
        std::shared_ptr<proto::Transaction> txn_ptr;
        std::unique_ptr<pack_params> pack_param;
        std::string csn_temp, key_temp, key_str, table_name, csn_result;
        uint64_t total_single_shard_time = 0, total_single_remote_handle_time = 0, total_single_shard_num = 0, total_single_remote_handle_num = 0;
        uint64_t thread_id = 0, local_server_id = 0, epoch_mod = 0, epoch = 0, max_length = 0, server_num = 1, shard_num = 0, replica_num = 1,
                round_robin = 0, sent_to = 0,///cache check
                message_epoch = 0, message_epoch_mod = 0, message_server_id = 0, txn_server_id = 0,shard_id = 0, shard_server_id =0, ///message epoch info
                server_reply_ack_id = 0,
                cache_clear_epoch_num = 0, cache_clear_epoch_num_mod = 0,
                redo_log_push_down_reply = 1;
        std::vector<std::vector<bool>> is_local_shard;
    public:
        bool res, sleep_flag;
        std::shared_ptr<proto::Transaction> empty_txn_ptr;
        std::hash<std::string> _hash;


    public:
        void Shard();

      bool UpdateMetaInfo();
};

}

#endif //TAAS_EPOCH_MESSAGE_RECEIVE_HANDLER_H
