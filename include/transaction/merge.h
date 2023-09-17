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

#include <cstdint>

namespace Taas {

    class Merger {

    public:
        std::unique_ptr<zmq::message_t> message_ptr;
        std::unique_ptr<std::string> message_string_ptr;
        std::unique_ptr<proto::Message> msg_ptr;
        std::shared_ptr<proto::Transaction> txn_ptr;
        std::unique_ptr<pack_params> pack_param;
        std::string csn_temp, key_temp, key_str, table_name, csn_result;
        uint64_t thread_id = 0, epoch = 0, epoch_mod = 0, txn_server_id = 0;
        bool res, sleep_flag;
        CRDTMerge merger;
        EpochMessageSendHandler message_transmitter;
        EpochMessageReceiveHandler message_handler;

        ///epoch
        static Context ctx;
        static AtomicCounters_Cache
                epoch_should_merge_txn_num, epoch_merged_txn_num,
                epoch_should_commit_txn_num, epoch_committed_txn_num,
                epoch_record_commit_txn_num, epoch_record_committed_txn_num;
        static std::vector<std::unique_ptr<concurrent_crdt_unordered_map<std::string, std::string, std::string>>>
                epoch_merge_map,
                local_epoch_abort_txn_set,
                epoch_abort_txn_set;

        /// whole server state
        static concurrent_unordered_map<std::string, std::string> read_version_map_data, read_version_map_csn, insert_set;

        ///queues
        static std::vector<std::unique_ptr<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>>
                epoch_merge_queue,///merge_queue 存放需要进行merge的子事务 不区分epoch
                epoch_commit_queue;///epoch_commit_queue 当前epoch的涉及当前分片的要进行validate和commit的子事务 receive from servers and local sharding txn, wait to validate

        static std::vector<std::unique_ptr<std::atomic<bool>>>
                epoch_merge_complete,
                epoch_commit_complete;

        static std::atomic<uint64_t> total_merge_txn_num, total_merge_latency, total_commit_txn_num, total_commit_latency, success_commit_txn_num, success_commit_latency,
            total_read_version_check_failed_txn_num, total_failed_txn_num;

        static std::condition_variable merge_cv, commit_cv;

        static void StaticInit(const Context& ctx_);
        static void ClearMergerEpochState(uint64_t &epoch);

        void Init(uint64_t id_);

        void Merge();
        void Commit();
        void EpochMerge();

        static void MergeQueueEnqueue(uint64_t &epoch, const std::shared_ptr<proto::Transaction>& txn_ptr);
        static bool MergeQueueTryDequeue(uint64_t &epoch, const std::shared_ptr<proto::Transaction>& txn_ptr);
        static void CommitQueueEnqueue(uint64_t &epoch, const std::shared_ptr<proto::Transaction>& txn_ptr);
        static bool CommitQueueTryDequeue(uint64_t &epoch, std::shared_ptr<proto::Transaction> txn_ptr);


        static bool CheckEpochMergeComplete(const uint64_t& epoch) {
            if(epoch_merge_complete[epoch % ctx.taasContext.kCacheMaxLength]->load()) {
                return true;
            }
            if (epoch < EpochManager::GetPhysicalEpoch() && IsMergeComplete(epoch)) {
                epoch_merge_complete[epoch % ctx.taasContext.kCacheMaxLength]->store(true);
                return true;
            }
            return false;
        }
        static bool IsEpochMergeComplete(const uint64_t& epoch) {
            return epoch_merge_complete[epoch % ctx.taasContext.kCacheMaxLength]->load();
        }

        static bool CheckEpochCommitComplete(const uint64_t& epoch) {
            if (epoch_commit_complete[epoch % ctx.taasContext.kCacheMaxLength]->load()) return true;
            if (epoch < EpochManager::GetPhysicalEpoch() && IsCommitComplete(epoch)) {
                epoch_commit_complete[epoch % ctx.taasContext.kCacheMaxLength]->store(true);
                return true;
            }
            return false;
        }
        static bool IsEpochCommitComplete(const uint64_t& epoch) {
            return epoch_commit_complete[epoch % ctx.taasContext.kCacheMaxLength]->load();
        }



        static bool IsMergeComplete(const uint64_t& epoch) {
            for(uint64_t i = 0; i < ctx.taasContext.kTxnNodeNum; i++) {
                if (epoch_should_merge_txn_num.GetCount(epoch, i) > epoch_merged_txn_num.GetCount(epoch, i))
                    return false;
            }
            return true;
        }
        static bool IsMergeComplete(const uint64_t &epoch, const uint64_t &server_id) {
            return epoch_should_merge_txn_num.GetCount(epoch, server_id) <= epoch_merged_txn_num.GetCount(epoch, server_id);
        }
        static bool IsCommitComplete(const uint64_t & epoch) {
            for(uint64_t i = 0; i < ctx.taasContext.kTxnNodeNum; i++) {
                if (epoch_should_commit_txn_num.GetCount(epoch, i) > epoch_committed_txn_num.GetCount(epoch, i))
                    return false;
            }
            return true;
        }
        static bool IsCommitComplete(const uint64_t & epoch, const uint64_t & server_id) {
            return epoch_should_commit_txn_num.GetCount(epoch, server_id) <= epoch_committed_txn_num.GetCount(epoch, server_id);
        }

        static bool IsRedoLogComplete(const uint64_t & epoch) {
            for(uint64_t i = 0; i < ctx.taasContext.kTxnNodeNum; i++) {
                if (epoch_record_commit_txn_num.GetCount(epoch, i) > epoch_record_committed_txn_num.GetCount(epoch, i))
                    return false;
            }
            return true;
        }
        static bool IsRedoLogComplete(const uint64_t & epoch, const uint64_t & server_id) {
            return epoch_record_commit_txn_num.GetCount(epoch, server_id) <= epoch_record_committed_txn_num.GetCount(epoch, server_id);
        }

    };
}

#endif //TAAS_MERGE_H
