//
// Created by 周慰星 on 11/9/22.
//

#ifndef TAAS_HANDLER_RECEIVE_H
#define TAAS_HANDLER_RECEIVE_H

#include "epoch/epoch_manager.h"
#include "tools/utilities.h"
#include "queue"

namespace Taas {
    class MessageReceiveHandler{
    public:
        bool Init(uint64_t id, Context context);
        bool HandleReceivedMessage();
        bool HandleReceivedTxn();
        bool Sharding();
        bool UpdateEpochAbortSet();
        bool CheckTxnReceiveComplete() const;
        bool HandleTxnCache();
        bool CheckReceivedStatesAndReply();

        uint64_t GetHashValue(const std::string& key) const {
            return _hash(key) % sharding_num;
        }

        static bool StaticInit(const Context& context);
        static bool Clear();
        static bool Clear(uint64_t epoch);
        bool ClearCache();
        bool ClearStaticCounters();
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
                counters_clear_epoch_num = 0, counters_clear_epoch_num_mod = 0;
        std::vector<uint64_t> backup_send_ack_epoch_num, backup_insert_set_send_ack_epoch_num, abort_set_send_ack_epoch_num; /// check and reply ack

        bool res, sleep_flag;

        Context ctx;
        proto::Transaction empty_txn;
        std::hash<std::string> _hash;

    public:
        ///sharding txns
        ///接收到来自client的事务，进行分片并将事务发送到指定的txn node
        static AtomicCounters_Cache ///epoch, server_id, num
            sharding_should_handle_local_txn_num, sharding_handled_local_txn_num,
            sharding_should_send_txn_num, sharding_send_txn_num;
        //other txn node sends shrading txn which should be merged in current txn node
        //sharding_cache[epoch][server_id(sharding_id)].queue
        std::vector<std::vector<std::unique_ptr<std::queue<std::unique_ptr<proto::Transaction>>>>> sharding_cache, local_txn_cache;
        static AtomicCounters_Cache ///epoch, index, value
            sharding_should_receive_pack_num, sharding_received_pack_num,
            sharding_should_receive_txn_num, sharding_received_txn_num,
            sharding_should_enqueue_merge_queue_txn_num, sharding_enqueued_merge_queue_txn_num,
            should_enqueue_local_txn_queue_txn_num, enqueued_local_txn_queue_txn_num;

        ///sharding backup txns
        //other txn node sends shrading txn backup to current txn node
        //backup_cache[epoch][sharding_id(server_id)].queue
        std::vector<std::vector<std::unique_ptr<std::queue<std::unique_ptr<proto::Transaction>>>>> backup_cache;
        static AtomicCounters_Cache ///epoch, index, value
            backup_should_send_txn_num, backup_send_txn_num,
            backup_should_receive_pack_num, backup_received_pack_num,
            backup_should_receive_txn_num, backup_received_txn_num,
            backup_received_ack_num;

        std::vector<std::vector<std::unique_ptr<std::queue<std::unique_ptr<proto::Transaction>>>>> insert_set_cache;
        static AtomicCounters_Cache ///epoch, index, value
            insert_set_should_receive_num, insert_set_received_num,
            insert_set_received_ack_num;


        std::vector<std::vector<std::unique_ptr<std::queue<std::unique_ptr<proto::Transaction>>>>> abort_set_cache;
        static AtomicCounters_Cache ///epoch, index, value
            sharding_should_receive_abort_set_num, sharding_received_abort_set_num,
            sharding_abort_set_received_ack_num;


        static bool IsShardingSendFinish(uint64_t epoch, uint64_t sharding_id) {
            return sharding_send_txn_num.GetCount(epoch, sharding_id) >= sharding_should_send_txn_num.GetCount(epoch, sharding_id)
             && epoch < EpochManager::GetPhysicalEpoch();
        }

        static bool IsBackUpSendFinish(uint64_t epoch, Context &ctx) {
            return backup_send_txn_num.GetCount(epoch) >= backup_should_send_txn_num.GetCount(epoch)
                   && epoch < EpochManager::GetPhysicalEpoch();
        }

        static bool IsRemoteShardingPackReceiveComplete(uint64_t epoch, Context &ctx) {
            for(int i = 0; i < (int)ctx.kTxnNodeNum; i ++) {
                if(i == (int)ctx.txn_node_ip_index || EpochManager::server_state.GetCount(i) == 0) continue;
                if(sharding_received_pack_num.GetCount(epoch, i) < EpochManager::server_state.GetCount(i)) return false;
            }
            return true;
        }

        static bool IsRemoteShardingTxnReceiveComplete(uint64_t epoch, Context &ctx) {
            for(int i = 0; i < (int)ctx.kTxnNodeNum; i ++) {
                if(i == (int)ctx.txn_node_ip_index || EpochManager::server_state.GetCount(i) == 0) continue;
                if(sharding_received_txn_num.GetCount(epoch, i) < sharding_received_txn_num.GetCount(epoch, i)) return false;
            }
            return true;
        }

        static bool IsRemoteAbortSetReceiveComplete(uint64_t epoch, Context &ctx) {
            for(int i = 0; i < (int)ctx.kTxnNodeNum; i ++) {
                if(i == (int)ctx.txn_node_ip_index || EpochManager::server_state.GetCount(i) == 0) continue;
                if(sharding_received_abort_set_num.GetCount(epoch, i) < EpochManager::server_state.GetCount(i)) return false;
            }
            return true;
        }

        static bool IsRemoteInsertSetReceiveComplete(uint64_t epoch, Context &ctx) {
            for(int i = 0; i < (int)ctx.kTxnNodeNum; i ++) {
                if(i == (int)ctx.txn_node_ip_index || EpochManager::server_state.GetCount(i) == 0) continue;
                if(insert_set_received_num.GetCount(epoch, i) < EpochManager::server_state.GetCount(i)) return false;
            }
            return true;
        }

        static bool IsEpochTxnEnqueued_MergeQueue(uint64_t epoch, Context &ctx) {
            for(int i = 0; i < (int)ctx.kTxnNodeNum; i ++) {
                if(i == (int)ctx.txn_node_ip_index || EpochManager::server_state.GetCount(i) == 0) continue;
                if(sharding_should_enqueue_merge_queue_txn_num.GetCount(epoch, i) >
                    sharding_enqueued_merge_queue_txn_num.GetCount(epoch, i)) return false;
            }
            return true;
        }

        static bool IsEpochTxnEnqueued_LocalTxnQueue(uint64_t epoch, Context &ctx) {
            for(int i = 0; i < (int)ctx.kTxnNodeNum; i ++) {
                if(i == (int)ctx.txn_node_ip_index || EpochManager::server_state.GetCount(i) == 0) continue;
                if(should_enqueue_local_txn_queue_txn_num.GetCount(epoch, i) >
                   enqueued_local_txn_queue_txn_num.GetCount(epoch, i)) return false;
            }
            return true;
        }

        static bool IsBackUpACKReceiveComplete(uint64_t epoch, Context &ctx) {
            for(int i = 0; i < (int)ctx.kTxnNodeNum; i ++) {
                if(i == (int)ctx.txn_node_ip_index || EpochManager::server_state.GetCount(i) == 0) continue;
                if(backup_received_ack_num.GetCount(epoch, i) <
                   1) return false;
            }
            return true;
        }

        static bool IsAbortSetACKReceiveComplete(uint64_t epoch, Context &ctx) {
            for(int i = 0; i < (int)ctx.kTxnNodeNum; i ++) {
                if(i == (int)ctx.txn_node_ip_index || EpochManager::server_state.GetCount(i) == 0) continue;
                if(sharding_abort_set_received_ack_num.GetCount(epoch, i) <
                   1) return false;
            }
            return true;
        }

        static bool IsInsertSetACKReceiveComplete(uint64_t epoch, Context &ctx) {
            for(int i = 0; i < (int)ctx.kTxnNodeNum; i ++) {
                if(i == (int)ctx.txn_node_ip_index || EpochManager::server_state.GetCount(i) == 0) continue;
                if(insert_set_received_ack_num.GetCount(epoch, i) <
                   1) return false;
            }
            return true;
        }
    };

}

#endif //TAAS_HANDLER_RECEIVE_H
