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
    static concurrent_unordered_map<std::string, std::unique_ptr<uint64_t>>
        row_lock_map;  /// key, tid

    // 将事务和元数据map
    static concurrent_unordered_map<std::string, std::unique_ptr<TwoPCTxnStateStruct>>
        txn_state_map;  /// tid, txn struct

    // 工具
    struct Comparator {
      bool operator()(const std::string& x1, const std::string& x2) {
        if (x1 == x2)
          return true;  // 相等返回true
        else if (x1.length() != x2.length())
          return x1.length() < x2.length();
        else
          return x1 < x2;
      }

    };

    uint64_t GetHashValue(const std::string& key) const { return _hash(key) % sharding_num; }
    // 生成key_sorted
    void GetKeySorted(proto::Transaction& txn) {
      for (uint64_t i = 0; i < txn.row_size(); i++) {
        key_sorted.insert(txn.row(i).key(), i);
      }
    }

    void ClientTxn_Init();
    bool Sharding_2PL();
    bool Two_PL_LOCK(proto::Transaction& txn);
    bool Two_PL_UNLOCK(proto::Transaction& txn);
    bool Check_2PL_complete(proto::Transaction& txn);
    bool Check_2PC_Prepare_complete(proto::Transaction& txn);
    bool Check_2PL_Commit_complete(proto::Transaction& txn);
    bool Send(const Context& ctx, uint64_t& epoch, uint64_t& to_whom, proto::Transaction& txn,
              proto::TxnType txn_type);
    bool SendToClient(const Context& ctx, proto::Transaction& txn, proto::TxnState txn_state);

    static bool Init(const Taas::Context& ctx_, uint64_t id);
    static bool HandleReceivedMessage();  // 处理接收到的消息
    static bool HandleReceivedTxn();      // 处理接收到的事务（coordinator/applicant）
    static bool SetMessageRelatedCountersInfo();

    std::unique_ptr<zmq::message_t> message_ptr;
    std::unique_ptr<std::string> message_string_ptr;
    std::unique_ptr<proto::Message> msg_ptr;
    std::unique_ptr<proto::Transaction> txn_ptr;
    std::unique_ptr<proto::Transaction> local_txn_ptr;
    std::unique_ptr<pack_params> pack_param;
    std::string csn_temp, key_temp, key_str, table_name, csn_result;
    uint64_t thread_id = 0, server_dequeue_id = 0, epoch_mod = 0, epoch = 0, max_length = 0,
             sharding_num = 0,                                              /// cache check
        message_epoch = 0, message_sharding_id = 0, message_server_id = 0;  /// message epoch info

    Context ctx;
    std::string tid;  // 记录当前tid
    // std::vector<std::string> key_sorted;
    std::map<std::string, uint64_t, Comparator> key_sorted;

    bool res, sleep_flag;

    std::hash<std::string> _hash;

    /// 从client得到的txn的tid
    // std::string tid_client, tid_to_remote, tid_from_remote;

    /// to_whom = all
    uint64_t to_whom_all = 0;

    uint64_t sharding_num_struct_progressing, two_pl_num_progressing,
        two_pc_prepare_num_progressing, two_pc_commit_num_progressing;
  };
}  // namespace Taas

#endif  // TAAS_TWO_PHASE_COMMIT_H
