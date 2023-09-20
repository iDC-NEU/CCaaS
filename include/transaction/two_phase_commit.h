//
// Created by user on 23-7-16.
//

#ifndef TAAS_TWO_PHASE_COMMIT_H
#define TAAS_TWO_PHASE_COMMIT_H
#pragma once

#include <zmq.hpp>

#include "epoch/epoch_manager.h"
#include "message/twoPC_message_receive_handler.h"
#include "message/twoPC_message_send_handler.h"
#include "proto/message.pb.h"
#include "tools/concurrent_hash_map.h"
#include "tools/utilities.h"

namespace Taas {
  
  class TwoPC {
  public:
    static concurrent_unordered_map<std::string, std::string>
        row_lock_map;  /// key, tid

    // 将事务和元数据map
    static concurrent_unordered_map<std::string, std::shared_ptr<TwoPCTxnStateStruct>>
        txn_state_map;  /// tid, txn struct

    // 工具
    struct Comparator {
      bool operator()(const std::string& x1, const std::string& x2) const{
        int int_a = std::stoi(x1);
        int int_b = std::stoi(x2);
        return int_a < int_b;
      }
    };

    uint64_t GetHashValue(const std::string& key) const {
        uint64_t hash_value = _hash(key);
        return hash_value % sharding_num;
    }
    // 生成key_sorted
    void GetKeySorted(proto::Transaction& txn) {
        key_sorted.clear();
      for (uint64_t i = 0; i < (uint64_t) txn.row_size(); i++) {
        key_sorted[txn.row(i).key()] =  i;
      }
    }

    void ClientTxn_Init();
    bool Sharding_2PL();
    bool Two_PL_LOCK(proto::Transaction& txn);
    bool Two_PL_UNLOCK(proto::Transaction& txn);
    bool Check_2PL_complete(proto::Transaction& txn, std::shared_ptr<TwoPCTxnStateStruct>);
    bool Check_2PC_Prepare_complete(proto::Transaction& txn, std::shared_ptr<TwoPCTxnStateStruct>);
    bool Check_2PC_Commit_complete(proto::Transaction& txn, std::shared_ptr<TwoPCTxnStateStruct>);
    bool Send(const Context& ctx, uint64_t& epoch, uint64_t& to_whom, proto::Transaction& txn,
              proto::TxnType txn_type);
    bool  SendToClient(const Context& ctx, proto::Transaction& txn, proto::TxnType txn_type,
                   proto::TxnState txn_state);
    static bool Init(const Taas::Context& ctx_, uint64_t id);
      bool HandleClientMessage();// 处理接收到的消息 from client
      bool HandleReceivedMessage();  // from coordinator

    bool HandleReceivedTxn();      // 处理接收到的事务（coordinator/applicant）
    bool SetMessageRelatedCountersInfo();


    // debug map
    static std::mutex mutex;
    void printSorted(){
        std::unique_lock<std::mutex> lock(mutex);
        std::cout << "current tid : " << tid << " === ";
        std::string tmp = "-1";
        for (auto iter = key_sorted.begin(); iter!=key_sorted.end();iter++) {
            row_lock_map.getValue(iter->first, tmp);
            if (tmp != "" && tmp != "0" && tmp != "-1"){
                std::cout <<"{ key : "<< iter->first <<" tid :" << tmp <<"} ";
            }
        }
        std::cout << std::endl;
        lock.unlock();
    }

    std::unique_ptr<zmq::message_t> message_ptr;
    std::unique_ptr<std::string> message_string_ptr;
    std::unique_ptr<proto::Message> msg_ptr;
    std::unique_ptr<proto::Transaction> txn_ptr;
    std::unique_ptr<proto::Transaction> local_txn_ptr;
    std::unique_ptr<pack_params> pack_param;
    std::string csn_temp, key_temp, key_str, table_name, csn_result;
    uint64_t thread_id, server_dequeue_id, epoch_mod, epoch, max_length,
                                                         /// cache check
        message_epoch, message_sharding_id, message_server_id;  /// message epoch info
    static uint64_t  sharding_num;
    static Context ctx;
    std::string tid;  // 记录当前tid
    std::map<std::string, uint64_t, Comparator> key_sorted; // first is the key/row

    bool res, sleep_flag;

    std::hash<std::string> _hash;

    /// 从client得到的txn的tid
    // std::string tid_client, tid_to_remote, tid_from_remote;

    /// to_whom = all
    uint64_t to_whom_all = 0;

    uint64_t sharding_num_struct_progressing, two_pl_num_progressing,
        two_pc_prepare_num_progressing, two_pc_commit_num_progressing;
    static std::atomic<uint64_t> successTxnNumber , totalTxnNumber, failedTxnNumber,
        lockFailed, unlockFailed;
  };

}  // namespace Taas

#endif  // TAAS_TWO_PHASE_COMMIT_H
