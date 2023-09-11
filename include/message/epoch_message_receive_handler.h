//
// Created by 周慰星 on 11/9/22.
//

#ifndef TAAS_EPOCH_MESSAGE_RECEIVE_HANDLER_H
#define TAAS_EPOCH_MESSAGE_RECEIVE_HANDLER_H

#pragma once

#include "epoch/epoch_manager.h"
#include "message/message.h"
#include "tools/utilities.h"

#include "zmq.hpp"

#include "queue"
#include "tools/thread_pool_light.h"

namespace Taas {

    class EpochMessageReceiveHandler{
    public:
        bool Init(const uint64_t &id);

        void HandleReceivedMessage();
        void HandleReceivedControlMessage();
        bool SetMessageRelatedCountersInfo();
        bool HandleReceivedTxn();
        bool HandleMultiModelClientTxn();
        bool HandleClientTxn();
        bool UpdateEpochAbortSet();

        uint64_t GetHashValue(const std::string& key) const {
            return _hash(key) % sharding_num;
        }

        static bool StaticInit(const Context& context);
        static bool StaticClear(uint64_t& epoch);

    private:
        std::unique_ptr<zmq::message_t> message_ptr;
        std::unique_ptr<std::string> message_string_ptr;
        std::unique_ptr<proto::Message> msg_ptr;
        std::shared_ptr<proto::Transaction> txn_ptr;
        std::unique_ptr<pack_params> pack_param;
        std::string csn_temp, key_temp, key_str, table_name, csn_result;
        uint64_t thread_id = 0,
                server_dequeue_id = 0, epoch_mod = 0, epoch = 0, max_length = 0,sharding_num = 0,///cache check
                message_epoch = 0, message_epoch_mod = 0, message_sharding_id = 0, message_server_id = 0, ///message epoch info
                server_reply_ack_id = 0,
                cache_clear_epoch_num = 0, cache_clear_epoch_num_mod = 0,
                redo_log_push_down_reply = 1;

        bool res, sleep_flag;
        std::shared_ptr<proto::Transaction> empty_txn_ptr;
        std::hash<std::string> _hash;

    public:
        static Context ctx;
        static std::vector<uint64_t>
            sharding_send_ack_epoch_num,
            backup_send_ack_epoch_num,
            backup_insert_set_send_ack_epoch_num,
            abort_set_send_ack_epoch_num; /// check and reply ack

        static std::vector<std::unique_ptr<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>>
            epoch_backup_txn,
            epoch_insert_set,
            epoch_abort_set;

        static concurrent_unordered_map<std::string, std::shared_ptr<MultiModelTxn>> multiModelTxnMap;

        static std::vector<std::unique_ptr<std::atomic<bool>>>
            epoch_sharding_send_complete,
            epoch_sharding_receive_complete,
            epoch_back_up_complete,
            epoch_abort_set_merge_complete,
            epoch_insert_set_complete;


        ///sharding txns
        ///接收到来自client的事务，进行分片并将事务发送到指定的txn node
        static AtomicCounters_Cache ///epoch, index, num
            sharding_should_handle_local_txn_num, sharding_handled_local_txn_num,
            sharding_should_handle_remote_txn_num, sharding_handled_remote_txn_num,
            sharding_should_send_txn_num, sharding_send_txn_num;

        ///这里需要注意 这几个计数器是以server_id为粒度增加的，不是线程id ！！！
        static AtomicCounters_Cache ///epoch, server_id, value
            ///remote sharding txn counters
            sharding_should_receive_pack_num, sharding_received_pack_num,
            sharding_should_receive_txn_num, sharding_received_txn_num,
            ///sharding ack
            sharding_received_ack_num,

            ///backup txn counters
            backup_should_send_txn_num, backup_send_txn_num,
            backup_should_receive_pack_num, backup_received_pack_num,
            backup_should_receive_txn_num, backup_received_txn_num,
            ///backup ack
            backup_received_ack_num,

            ///insert set counters
            insert_set_should_receive_num, insert_set_received_num,
            ///insert set ack
            insert_set_received_ack_num,

            ///abort set counters
            abort_set_should_receive_num, abort_set_received_num,
            ///abort set ack
            abort_set_received_ack_num,
            ///redo log push down state
            redo_log_push_down_ack_num,
            redo_log_push_down_local_epoch;

        static std::condition_variable txn_cv, epoch_cv;

        static bool CheckEpochShardingSendComplete(const uint64_t& epoch) {
            if(epoch_sharding_send_complete[epoch % ctx.taasContext.kCacheMaxLength]->load()) {
                return true;
            }
            if (epoch < EpochManager::GetPhysicalEpoch() &&
                    IsShardingACKReceiveComplete(epoch) &&
                    IsShardingSendFinish(epoch)
                    ) {
                epoch_sharding_send_complete[epoch % ctx.taasContext.kCacheMaxLength]->store(true);
                return true;
            }
            return false;
        }

        static bool CheckEpochShardingReceiveComplete(uint64_t& epoch) {
            if (epoch_sharding_receive_complete[epoch % ctx.taasContext.kCacheMaxLength]->load()) return true;
            if (epoch < EpochManager::GetPhysicalEpoch() &&
                IsShardingPackReceiveComplete(epoch) &&
                    IsShardingTxnReceiveComplete(epoch)) {
                epoch_sharding_receive_complete[epoch % ctx.taasContext.kCacheMaxLength]->store(true);
                return true;
            }
            return false;
        }

        static bool CheckEpochBackUpComplete(uint64_t& epoch) {
            if (epoch_back_up_complete[epoch % ctx.taasContext.kCacheMaxLength]->load()) return true;
            if(epoch < EpochManager::GetPhysicalEpoch() && IsBackUpACKReceiveComplete(epoch)
                &&IsBackUpSendFinish(epoch)) {
                epoch_back_up_complete[epoch % ctx.taasContext.kCacheMaxLength]->store(true);
                return true;
            }
            return false;
        }

        static bool CheckEpochAbortSetMergeComplete(uint64_t& epoch) {
            if(epoch_abort_set_merge_complete[epoch % ctx.taasContext.kCacheMaxLength]->load()) return true;
            if(epoch < EpochManager::GetPhysicalEpoch() &&
                IsAbortSetACKReceiveComplete(epoch) &&
                    IsAbortSetReceiveComplete(epoch)
            ) {
                epoch_abort_set_merge_complete[epoch % ctx.taasContext.kCacheMaxLength]->store(true);
                return true;
            }
            return false;
        }

        static bool CheckEpochInsertSetMergeComplete(uint64_t& epoch) {
            if(epoch_insert_set_complete[epoch % ctx.taasContext.kCacheMaxLength]->load()) return true;
            if(epoch < EpochManager::GetPhysicalEpoch() &&
               IsInsertSetACKReceiveComplete(epoch) &&
               IsInsertSetReceiveComplete(epoch)
                    ) {
                epoch_insert_set_complete[epoch % ctx.taasContext.kCacheMaxLength]->store(true);
                return true;
            }
            return false;
        }


        ///local txn sharding send check
        static bool IsShardingSendFinish(const uint64_t &epoch, const uint64_t &sharding_id) {
            return epoch < EpochManager::GetPhysicalEpoch() &&
                    sharding_send_txn_num.GetCount(epoch, sharding_id) >= sharding_should_send_txn_num.GetCount(epoch, sharding_id) &&
                    sharding_handled_local_txn_num.GetCount(epoch) >= sharding_should_handle_local_txn_num.GetCount(epoch);
        }
        static bool IsShardingSendFinish(const uint64_t &epoch) {
            return epoch < EpochManager::GetPhysicalEpoch() &&
                    sharding_send_txn_num.GetCount(epoch) >= sharding_should_send_txn_num.GetCount(epoch) &&
                    sharding_handled_local_txn_num.GetCount(epoch) >= sharding_should_handle_local_txn_num.GetCount(epoch);
        }
        static bool IsShardingACKReceiveComplete(const uint64_t &epoch) {
            for(uint64_t i = 0; i < ctx.taasContext.kTxnNodeNum; i ++) {
                if(i == ctx.taasContext.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
                if(sharding_received_ack_num.GetCount(epoch, i) < sharding_should_receive_pack_num.GetCount(epoch, i)) return false;
            }
            return true;
        }
        static bool IsEpochTxnHandleComplete(const uint64_t &epoch) {
            return epoch < EpochManager::GetPhysicalEpoch() &&
                sharding_handled_local_txn_num.GetCount(epoch) >= sharding_should_handle_local_txn_num.GetCount(epoch) &&
                    sharding_handled_remote_txn_num.GetCount(epoch) >= sharding_should_handle_remote_txn_num.GetCount(epoch);
        }
        ///remote txn receive check
        static bool IsShardingPackReceiveComplete(const uint64_t &epoch) {
            for(uint64_t i = 0; i < ctx.taasContext.kTxnNodeNum; i ++) {
                if(i == ctx.taasContext.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
                if(sharding_received_pack_num.GetCount(epoch, i) < sharding_should_receive_pack_num.GetCount(epoch, i)) return false;
            }
            return true;
        }
        static bool IsShardingPackReceiveComplete(const uint64_t &epoch, const uint64_t &id) {
            return sharding_received_pack_num.GetCount(epoch, id) >= sharding_should_receive_pack_num.GetCount(epoch, id);
        }
        static bool IsShardingTxnReceiveComplete(const uint64_t &epoch) {
            for(uint64_t i = 0; i < ctx.taasContext.kTxnNodeNum; i ++) {
                if(i == ctx.taasContext.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
                if(sharding_received_txn_num.GetCount(epoch, i) < sharding_received_txn_num.GetCount(epoch, i)) return false;
            }
            return true;
        }
        static bool IsShardingTxnReceiveComplete(const uint64_t &epoch, const uint64_t &id) {
            return sharding_received_txn_num.GetCount(epoch, id) >= sharding_received_txn_num.GetCount(epoch, id);
        }




        ///backup check
        static bool IsBackUpSendFinish(const uint64_t &epoch) {
            return epoch < EpochManager::GetPhysicalEpoch() &&
                    backup_send_txn_num.GetCount(epoch) >= backup_should_send_txn_num.GetCount(epoch) &&
                    sharding_handled_local_txn_num.GetCount(epoch) >= sharding_should_handle_local_txn_num.GetCount(epoch);
        }
        static bool IsBackUpACKReceiveComplete(const uint64_t &epoch) {
            uint64_t to_id ;
            for(uint64_t i = 0; i < ctx.taasContext.kBackUpNum; i ++) { /// send to i+1, i+2...i+kBackNum-1
                to_id = (ctx.taasContext.txn_node_ip_index + i + 1) % ctx.taasContext.kTxnNodeNum;
                if(to_id == (uint64_t)ctx.taasContext.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, to_id) == 0) continue;
                if(backup_received_ack_num.GetCount(epoch, to_id) < backup_should_receive_pack_num.GetCount(epoch, to_id)) return false;
            }
            return true;
        }
        static bool IsBackUpPackReceiveComplete(const uint64_t &epoch) {
            for(uint64_t i = 0; i < ctx.taasContext.kTxnNodeNum; i ++) {
                if(i == ctx.taasContext.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
                if(backup_received_pack_num.GetCount(epoch, i) < backup_should_receive_pack_num.GetCount(epoch, i)) return false;
            }
            return true;
        }
        static bool IsBackUpPackReceiveComplete(const uint64_t &epoch, const uint64_t &id) {
            return backup_received_pack_num.GetCount(epoch, id) >= backup_should_receive_pack_num.GetCount(epoch, id);
        }
        static bool IsBackUpTxnReceiveComplete(const uint64_t &epoch) {
            uint64_t to_id;
            for(uint64_t i = 0; i < ctx.taasContext.kBackUpNum; i ++) { /// send to i+1, i+2...i+kBackNum-1
                to_id = (ctx.taasContext.txn_node_ip_index + i + 1) % ctx.taasContext.kTxnNodeNum;
                if(to_id == ctx.taasContext.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, to_id) == 0) continue;
                if(backup_received_txn_num.GetCount(epoch, to_id) < backup_received_txn_num.GetCount(epoch, to_id)) return false;
            }
            return true;
        }
        static bool IsBackUpTxnReceiveComplete(const uint64_t &epoch, const uint64_t &id) {
            return backup_received_txn_num.GetCount(epoch, id) >= backup_received_txn_num.GetCount(epoch, id);
        }




        ///abort set check
        static bool IsAbortSetReceiveComplete(const uint64_t &epoch, const uint64_t &id) {
            return abort_set_received_num.GetCount(epoch, id) >= abort_set_should_receive_num.GetCount(epoch, id);
        }
        static bool IsAbortSetReceiveComplete(const uint64_t &epoch) {
            for(uint64_t i = 0; i < ctx.taasContext.kTxnNodeNum; i ++) {
                if(i == ctx.taasContext.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
                if(abort_set_received_num.GetCount(epoch, i) < abort_set_should_receive_num.GetCount(epoch, i)) return false;
            }
            return true;
        }
        static bool IsAbortSetACKReceiveComplete(const uint64_t &epoch) {
            for(uint64_t i = 0; i < ctx.taasContext.kTxnNodeNum; i ++) {
                if(i == ctx.taasContext.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
                if(abort_set_received_ack_num.GetCount(epoch, i) < abort_set_should_receive_num.GetCount(epoch, i)) return false;
            }
            return true;
        }
        ///insert set check
        static bool IsInsertSetReceiveComplete(const uint64_t &epoch, const uint64_t &id) {
            return insert_set_received_num.GetCount(epoch, id) >= insert_set_should_receive_num.GetCount(epoch, id);
        }
        static bool IsInsertSetReceiveComplete(const uint64_t &epoch) {
            for(uint64_t i = 0; i < ctx.taasContext.kTxnNodeNum; i ++) {
                if(i == ctx.taasContext.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
                if(insert_set_received_num.GetCount(epoch, i) < insert_set_should_receive_num.GetCount(epoch, i)) return false;
            }
            return true;
        }
        static bool IsInsertSetACKReceiveComplete(const uint64_t &epoch) {
            for(uint64_t i = 0; i < ctx.taasContext.kTxnNodeNum; i ++) {
                if(i == ctx.taasContext.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
                if(insert_set_received_ack_num.GetCount(epoch, i) < insert_set_should_receive_num.GetCount(epoch, i)) return false;
            }
            return true;
        }
        ///redo log check
        static bool IsRedoLogPushDownACKReceiveComplete(const uint64_t &epoch) {
            for(uint64_t i = 0; i < ctx.taasContext.kTxnNodeNum; i ++) {
                if(i == ctx.taasContext.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
                if(redo_log_push_down_ack_num.GetCount(epoch, i) < EpochManager::server_state.GetCount(epoch, i)) return false;
            }
            return true;
        }


        void HandleMultiModelClientSubTxn();

        uint64_t getMultiModelTxnId();
    };

}

#endif //TAAS_EPOCH_MESSAGE_RECEIVE_HANDLER_H
