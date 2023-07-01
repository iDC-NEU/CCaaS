//
// Created by 周慰星 on 11/9/22.
//

#ifndef TAAS_HANDLER_RECEIVE_H
#define TAAS_HANDLER_RECEIVE_H

#pragma once

#include "epoch/epoch_manager.h"
#include "tools/utilities.h"

#include "queue"

namespace Taas {
    class MessageReceiveHandler{
    public:
        bool Init(const Context& ctx_, uint64_t id);
        bool SetMessageRelatedCountersInfo();

        bool HandleReceivedEpochMessage();
        void HandleReceivedEpochMessage_Usleep();
        void HandleReceivedEpochMessage_Block();

        bool HandleReceivedTxnMessage();
        void HandleReceivedTxnMessage_Usleep();
        void HandleReceivedTxnMessage_Block();

        bool HandleReceivedTxn();

        bool Sharding();
        bool UpdateEpochAbortSet();
        bool CheckReceivedStatesAndReply();

        uint64_t GetHashValue(const std::string& key) const {
            return _hash(key) % sharding_num;
        }

        static bool StaticInit(const Context& context);
        static bool StaticClear(const Context& context, uint64_t& epoch);
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

    public:
        static std::vector<uint64_t>
            sharding_send_ack_epoch_num,
            backup_send_ack_epoch_num,
            backup_insert_set_send_ack_epoch_num,
            abort_set_send_ack_epoch_num; /// check and reply ack

        static std::vector<std::unique_ptr<BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>>
            epoch_remote_sharding_txn,
            epoch_local_sharding_txn,
            epoch_local_txn,
            epoch_backup_txn,
            epoch_insert_set,
            epoch_abort_set;

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

        static bool CheckEpochShardingSendComplete(const Context &ctx, uint64_t& epoch) {
            if(epoch_sharding_send_complete[epoch % ctx.kCacheMaxLength]->load()) {
                return true;
            }
            if (epoch < EpochManager::GetPhysicalEpoch() &&
                    IsShardingACKReceiveComplete(ctx, epoch) &&
                    IsShardingSendFinish(epoch)
                    ) {
                epoch_sharding_send_complete[epoch % ctx.kCacheMaxLength]->store(true);
                return true;
            }
            return false;
        }

        static bool CheckEpochShardingReceiveComplete(const Context &ctx, uint64_t& epoch) {
            if (epoch_sharding_receive_complete[epoch % ctx.kCacheMaxLength]->load()) return true;
            if (epoch < EpochManager::GetPhysicalEpoch() &&
                IsShardingPackReceiveComplete(ctx, epoch) &&
                    IsShardingTxnReceiveComplete(ctx, epoch)) {
                epoch_sharding_receive_complete[epoch % ctx.kCacheMaxLength]->store(true);
                return true;
            }
            return false;
        }

        static bool CheckEpochBackUpComplete(const Context &ctx, uint64_t& epoch) {
            if (epoch_back_up_complete[epoch % ctx.kCacheMaxLength]->load()) return true;
            if(epoch < EpochManager::GetPhysicalEpoch() && IsBackUpACKReceiveComplete(ctx, epoch)
                &&IsBackUpSendFinish(epoch)) {
                epoch_back_up_complete[epoch % ctx.kCacheMaxLength]->store(true);
                return true;
            }
            return false;
        }

        static bool CheckEpochAbortSetMergeComplete(const Context &ctx, uint64_t& epoch) {
            if(epoch_abort_set_merge_complete[epoch % ctx.kCacheMaxLength]->load()) return true;
            if(epoch < EpochManager::GetPhysicalEpoch() &&
                IsAbortSetACKReceiveComplete(ctx, epoch) &&
                    IsAbortSetReceiveComplete(ctx, epoch)
            ) {
                epoch_abort_set_merge_complete[epoch % ctx.kCacheMaxLength]->store(true);
                return true;
            }
            return false;
        }

        static bool CheckEpochInsertSetMergeComplete(const Context &ctx, uint64_t& epoch) {
            if(epoch_insert_set_complete[epoch % ctx.kCacheMaxLength]->load()) return true;
            if(epoch < EpochManager::GetPhysicalEpoch() &&
               IsInsertSetACKReceiveComplete(ctx, epoch) &&
               IsInsertSetReceiveComplete(ctx, epoch)
                    ) {
                epoch_insert_set_complete[epoch % ctx.kCacheMaxLength]->store(true);
                return true;
            }
            return false;
        }


        ///local txn sharding send check
        static bool IsShardingSendFinish(uint64_t epoch, uint64_t sharding_id) {
            return epoch < EpochManager::GetPhysicalEpoch() &&
                    sharding_send_txn_num.GetCount(epoch, sharding_id) >= sharding_should_send_txn_num.GetCount(epoch, sharding_id) &&
                    sharding_handled_local_txn_num.GetCount(epoch) >= sharding_should_handle_local_txn_num.GetCount(epoch);
        }
        static bool IsShardingSendFinish(uint64_t epoch) {
            return epoch < EpochManager::GetPhysicalEpoch() &&
                    sharding_send_txn_num.GetCount(epoch) >= sharding_should_send_txn_num.GetCount(epoch) &&
                    sharding_handled_local_txn_num.GetCount(epoch) >= sharding_should_handle_local_txn_num.GetCount(epoch);
        }
        static bool IsShardingACKReceiveComplete(const Context &ctx, uint64_t epoch) {
            for(uint64_t i = 0; i < ctx.kTxnNodeNum; i ++) {
                if(i == ctx.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
                if(sharding_received_ack_num.GetCount(epoch, i) < sharding_should_receive_pack_num.GetCount(epoch, i)) return false;
            }
            return true;
        }
        static bool IsEpochTxnHandleComplete(uint64_t epoch) {
            return epoch < EpochManager::GetPhysicalEpoch() &&
                sharding_handled_local_txn_num.GetCount(epoch) >= sharding_should_handle_local_txn_num.GetCount(epoch) &&
                    sharding_handled_remote_txn_num.GetCount(epoch) >= sharding_should_handle_remote_txn_num.GetCount(epoch);
        }
        ///remote txn receive check
        static bool IsShardingPackReceiveComplete(const Context &ctx, uint64_t epoch) {
            for(uint64_t i = 0; i < ctx.kTxnNodeNum; i ++) {
                if(i == ctx.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
                if(sharding_received_pack_num.GetCount(epoch, i) < sharding_should_receive_pack_num.GetCount(epoch, i)) return false;
            }
            return true;
        }
        static bool IsShardingPackReceiveComplete(uint64_t epoch, uint64_t id) {
            return sharding_received_pack_num.GetCount(epoch, id) >= sharding_should_receive_pack_num.GetCount(epoch, id);
        }
        static bool IsShardingTxnReceiveComplete(const Context &ctx, uint64_t epoch) {
            for(uint64_t i = 0; i < ctx.kTxnNodeNum; i ++) {
                if(i == ctx.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
                if(sharding_received_txn_num.GetCount(epoch, i) < sharding_received_txn_num.GetCount(epoch, i)) return false;
            }
            return true;
        }
        static bool IsShardingTxnReceiveComplete(uint64_t epoch, uint64_t id) {
            return sharding_received_txn_num.GetCount(epoch, id) >= sharding_received_txn_num.GetCount(epoch, id);
        }




        ///backup check
        static bool IsBackUpSendFinish(uint64_t epoch) {
            return epoch < EpochManager::GetPhysicalEpoch() &&
                    backup_send_txn_num.GetCount(epoch) >= backup_should_send_txn_num.GetCount(epoch) &&
                    sharding_handled_local_txn_num.GetCount(epoch) >= sharding_should_handle_local_txn_num.GetCount(epoch);
        }
        static bool IsBackUpACKReceiveComplete(const Context &ctx, uint64_t epoch) {
            auto to_id = ctx.txn_node_ip_index ;
            for(uint64_t i = 0; i < ctx.kBackUpNum; i ++) { /// send to i+1, i+2...i+kBackNum-1
                to_id = (ctx.txn_node_ip_index + i + 1) % ctx.kTxnNodeNum;
                if(to_id == (uint64_t)ctx.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, to_id) == 0) continue;
                if(backup_received_ack_num.GetCount(epoch, to_id) < backup_should_receive_pack_num.GetCount(epoch, to_id)) return false;
            }
            return true;
        }
        static bool IsBackUpPackReceiveComplete(const Context &ctx, uint64_t epoch) {
            for(uint64_t i = 0; i < ctx.kTxnNodeNum; i ++) {
                if(i == ctx.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
                if(backup_received_pack_num.GetCount(epoch, i) < backup_should_receive_pack_num.GetCount(epoch, i)) return false;
            }
            return true;
        }
        static bool IsBackUpPackReceiveComplete(uint64_t epoch, uint64_t id) {
            return backup_received_pack_num.GetCount(epoch, id) >= backup_should_receive_pack_num.GetCount(epoch, id);
        }
        static bool IsBackUpTxnReceiveComplete(const Context &ctx, uint64_t epoch) {
            auto to_id = ctx.txn_node_ip_index;
            for(uint64_t i = 0; i < ctx.kBackUpNum; i ++) { /// send to i+1, i+2...i+kBackNum-1
                to_id = (ctx.txn_node_ip_index + i + 1) % ctx.kTxnNodeNum;
                if(to_id == ctx.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, to_id) == 0) continue;
                if(backup_received_txn_num.GetCount(epoch, to_id) < backup_received_txn_num.GetCount(epoch, to_id)) return false;
            }
            return true;
        }
        static bool IsBackUpTxnReceiveComplete(uint64_t epoch, uint64_t id) {
            return backup_received_txn_num.GetCount(epoch, id) >= backup_received_txn_num.GetCount(epoch, id);
        }




        ///abort set check
        static bool IsAbortSetReceiveComplete(uint64_t epoch, uint64_t id) {
            return abort_set_received_num.GetCount(epoch, id) >= abort_set_should_receive_num.GetCount(epoch, id);
        }
        static bool IsAbortSetReceiveComplete(const Context &ctx, uint64_t epoch) {
            for(uint64_t i = 0; i < ctx.kTxnNodeNum; i ++) {
                if(i == ctx.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
                if(abort_set_received_num.GetCount(epoch, i) < abort_set_should_receive_num.GetCount(epoch, i)) return false;
            }
            return true;
        }
        static bool IsAbortSetACKReceiveComplete(const Context &ctx, uint64_t epoch) {
            for(uint64_t i = 0; i < ctx.kTxnNodeNum; i ++) {
                if(i == ctx.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
                if(abort_set_received_ack_num.GetCount(epoch, i) < abort_set_should_receive_num.GetCount(epoch, i)) return false;
            }
            return true;
        }
        ///insert set check
        static bool IsInsertSetReceiveComplete(uint64_t epoch, uint64_t id) {
            return insert_set_received_num.GetCount(epoch, id) >= insert_set_should_receive_num.GetCount(epoch, id);
        }
        static bool IsInsertSetReceiveComplete(const Context &ctx, uint64_t epoch) {
            for(uint64_t i = 0; i < ctx.kTxnNodeNum; i ++) {
                if(i == ctx.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
                if(insert_set_received_num.GetCount(epoch, i) < insert_set_should_receive_num.GetCount(epoch, i)) return false;
            }
            return true;
        }
        static bool IsInsertSetACKReceiveComplete(const Context &ctx, uint64_t epoch) {
            for(uint64_t i = 0; i < ctx.kTxnNodeNum; i ++) {
                if(i == ctx.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
                if(insert_set_received_ack_num.GetCount(epoch, i) < insert_set_should_receive_num.GetCount(epoch, i)) return false;
            }
            return true;
        }
        ///redo log check
        static bool IsRedoLogPushDownACKReceiveComplete(const Context &ctx, uint64_t epoch) {
            for(uint64_t i = 0; i < ctx.kTxnNodeNum; i ++) {
                if(i == ctx.txn_node_ip_index || EpochManager::server_state.GetCount(epoch, i) == 0) continue;
                if(redo_log_push_down_ack_num.GetCount(epoch, i) < EpochManager::server_state.GetCount(epoch, i)) return false;
            }
            return true;
        }



    };

}

#endif //TAAS_HANDLER_RECEIVE_H
