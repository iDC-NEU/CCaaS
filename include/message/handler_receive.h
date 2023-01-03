//
// Created by 周慰星 on 11/9/22.
//

#ifndef TAAS_HANDLER_RECEIVE_H
#define TAAS_HANDLER_RECEIVE_H

#include "epoch/epoch_manager.h"
#include "utils/utilities.h"

namespace Taas {
    class MessageReceiveHandler{
    public:
        bool Init(uint64_t id);
        bool HandleReceivedMessage();
        bool HandleReceivedClientTxn();
        bool HandleReceivedTxn();
        bool Sharding();
        bool UpdateEpochAbortSet();
        bool CheckTxnReceiveComplete() const;
        bool HandleTxnCache();

        uint64_t GetHashValue(const std::string& key) const {
            return _hash(key) % sharding_num;
        }

        static bool StaticInit(Context context);
        static bool Clear();
        static bool Clear(uint64_t epoch);
    private:
        std::unique_ptr<zmq::message_t> message_ptr;
        std::unique_ptr<std::string> message_string_ptr;
        std::unique_ptr<proto::Message> msg_ptr;
        std::unique_ptr<proto::Transaction> txn_ptr;
        std::unique_ptr<pack_params> pack_param;
        std::string csn_temp, key_temp, key_str, table_name, csn_result;
        uint64_t thread_id = 0, server_dequeue_id = 0, epoch_mod = 0,
                epoch = 0, clear_epoch = 0,max_length = 0, sharding_num = 0;
        bool res, sleep_flag;

        static Context ctx;

        std::hash<std::string> _hash;

    public:
        ///接收到来自client的事务，进行分片并将事务发送到指定的txn node
        static AtomicCounters_Cache ///epoch, server_id, num
            sharding_should_handle_local_txn_num, sharding_handled_local_txn_num,
            sharding_should_send_txn_num, sharding_send_txn_num;
        //other txn node sends shrading txn which should be merged in current txn node
        //sharding_cache[epoch][server_id(sharding_id)].queue
        std::vector<std::vector<std::unique_ptr<std::queue<std::unique_ptr<proto::Transaction>>>>> sharding_cache, local_txn_cache;
        //sharding counters
        static AtomicCounters_Cache ///epoch, index, value
            sharding_should_receive_pack_num, sharding_received_pack_num,
            sharding_should_receive_txn_num, sharding_received_txn_num,
            sharding_should_enqueue_merge_queue_txn_num, sharding_enqueued_merge_queue_txn_num,
            should_enqueue_local_txn_queue_txn_num, enqueued_local_txn_queue_txn_num;

        //other txn node sends shrading txn backup to current txn node
        //backup_cache[epoch][sharding_id(server_id)].queue
        std::vector<std::vector<std::unique_ptr<std::queue<std::unique_ptr<proto::Transaction>>>>> backup_cache;
        static AtomicCounters_Cache ///epoch, index, value
            backup_should_send_txn_num, backup_send_txn_num,
            backup_should_receive_pack_num, backup_received_pack_num,
            backup_should_receive_txn_num, backup_received_txn_num;

        std::vector<std::vector<std::unique_ptr<std::queue<std::unique_ptr<proto::Transaction>>>>> backup_insert_set_cache;
        static AtomicCounters_Cache ///epoch, index, value
            backup_insert_set_should_receive_num, backup_insert_set_received_num;

        std::vector<std::vector<std::unique_ptr<std::queue<std::unique_ptr<proto::Transaction>>>>> abort_set_cache;
        static AtomicCounters_Cache ///epoch, index, value
            sharding_should_receive_abort_set_num, sharding_received_abort_set_num;

        static bool IsShardingSendFinish(uint64_t epoch, uint64_t sharding_id) {
            return sharding_send_txn_num.GetCount(epoch, sharding_id) >= sharding_should_send_txn_num.GetCount(epoch, sharding_id)
             && epoch < EpochManager::GetPhysicalEpoch();
        }

        static bool IsBackUpSendFinish(uint64_t epoch) {
            return backup_send_txn_num.GetCount(epoch) >= backup_should_send_txn_num.GetCount(epoch)
                   && epoch < EpochManager::GetPhysicalEpoch();
        }

        static bool RemoteShardingPackReceiveComplete(uint64_t epoch) {
            for(int i = 0; i < (int)ctx.kTxnNodeNum; i ++) {
                if(i == (int)ctx.txn_node_ip_index || EpochManager::server_state.GetCount(i) == 0) continue;
                if(sharding_should_receive_pack_num.GetCount(epoch, i) < EpochManager::server_state.GetCount(i)) return false;
            }
            return true;
        }

        static bool RemoteShardingTxnReceiveComplete(uint64_t epoch) {
            for(int i = 0; i < (int)ctx.kTxnNodeNum; i ++) {
                if(i == (int)ctx.txn_node_ip_index || EpochManager::server_state.GetCount(i) == 0) continue;
                if(sharding_received_txn_num.GetCount(epoch, i) < sharding_received_txn_num.GetCount(epoch, i)) return false;
            }
            return true;
        }

        static bool RemoteAbortSetReceiveComplete(uint64_t epoch) {
            for(int i = 0; i < (int)ctx.kTxnNodeNum; i ++) {
                if(i == (int)ctx.txn_node_ip_index || EpochManager::server_state.GetCount(i) == 0) continue;
                if(sharding_received_abort_set_num.GetCount(epoch, i) < EpochManager::server_state.GetCount(i)) return false;
            }
            return true;
        }

        static bool RemoteInsertSetReceiveComplete(uint64_t epoch) {
            for(int i = 0; i < (int)ctx.kTxnNodeNum; i ++) {
                if(i == (int)ctx.txn_node_ip_index || EpochManager::server_state.GetCount(i) == 0) continue;
                if(backup_insert_set_received_num.GetCount(epoch, i) < EpochManager::server_state.GetCount(i)) return false;
            }
            return true;
        }

        static bool EpochTxnEnqeueud_MergeQueue(uint64_t epoch) {
            for(int i = 0; i < (int)ctx.kTxnNodeNum; i ++) {
                if(i == (int)ctx.txn_node_ip_index || EpochManager::server_state.GetCount(i) == 0) continue;
                if(sharding_should_enqueue_merge_queue_txn_num.GetCount(epoch, i) <
                    sharding_enqueued_merge_queue_txn_num.GetCount(epoch, i)) return false;
            }
            return true;
        }

        static bool EpochTxnEnqeueud_LocalTxnQueue(uint64_t epoch) {
            for(int i = 0; i < (int)ctx.kTxnNodeNum; i ++) {
                if(i == (int)ctx.txn_node_ip_index || EpochManager::server_state.GetCount(i) == 0) continue;
                if(should_enqueue_local_txn_queue_txn_num.GetCount(epoch, i) <
                   enqueued_local_txn_queue_txn_num.GetCount(epoch, i)) return false;
            }
            return true;
        }

    };

}

#endif //TAAS_HANDLER_RECEIVE_H
