//
// Created by 周慰星 on 11/15/22.
//

#ifndef TAAS_MERGE_H
#define TAAS_MERGE_H

#pragma once

#include "transaction/crdt_merge.h"
#include "message/epoch_message_send_handler.h"
#include "message/epoch_message_receive_handler.h"

#include "zmq.hpp"
#include "proto/message.pb.h"
#include "tools/thread_counters.h"

#include <cstdint>

namespace Taas {

    class Merger : public ThreadCounters {

    public:
        std::unique_ptr<zmq::message_t> message_ptr;
        std::unique_ptr<std::string> message_string_ptr;
        std::unique_ptr<proto::Message> msg_ptr;
        std::shared_ptr<proto::Transaction> txn_ptr, write_set, backup_txn, full_txn;
        std::shared_ptr<std::vector<std::shared_ptr<proto::Transaction>>> shard_row_vector;
        std::unique_ptr<pack_params> pack_param;
        std::string csn_temp, key_temp, key_str, table_name, csn_result;
        uint64_t local_server_id, epoch_mod = 0, epoch = 0, max_length = 0, server_num = 1, shard_id = 0, shard_server_id =0, replica_num = 1,
                round_robin = 0, sent_to = 0,///cache check
        message_epoch = 0, message_epoch_mod = 0, message_server_id = 0, ///message epoch info
        txn_server_id = 0;

        bool res, sleep_flag;
        std::shared_ptr<proto::Transaction> empty_txn_ptr;
        std::hash<std::string> _hash;

        uint64_t total_single_shard_time = 0,
        total_single_remote_handle_time = 0,
        total_single_validate_time = 0,
        total_single_merge_time = 0,
        total_single_abort_set_time = 0,
        total_single_commit_time = 0,
        total_single_log_time = 0,
        total_single_result_time = 0,
        total_single_time = 0,

        total_single_shard_num = 0,
        total_single_remote_handle_num = 0,
        total_single_validate_num = 0,
        total_single_merge_num = 0,
        total_single_abort_set_num = 0,
        total_single_commit_num = 0,
        total_single_log_num = 0,
        total_single_result_num = 0,
        total_single_num = 0;

        [[nodiscard]] uint64_t GetHashValue(const std::string& key) const {
            return _hash(key) % shard_num;
        }

    public:
        bool MergeQueueTryDequeue(uint64_t &epoch_, const std::shared_ptr<proto::Transaction>& txn_ptr_);
        bool CommitQueueTryDequeue(uint64_t &epoch_, std::shared_ptr<proto::Transaction> txn_ptr_);

        void MergeInit(const uint64_t &id);
        void ReadValidate();
        void Send();
        void Merge();
        void Commit();
        void RedoLog();
        void ResultReturn();
        void EpochMerge();

        void ReadValidateQueueEnqueue(uint64_t &epoch_, const std::shared_ptr<proto::Transaction> &txn_ptr_);
        void MergeQueueEnqueue(uint64_t &epoch_, const std::shared_ptr<proto::Transaction>& txn_ptr_);
        void CommitQueueEnqueue(uint64_t &epoch_, const std::shared_ptr<proto::Transaction>& txn_ptr_);
        void ResultReturnQueueEnqueue(uint64_t &epoch_, const std::shared_ptr<proto::Transaction>& txn_ptr_);

    };
}

#endif //TAAS_MERGE_H
