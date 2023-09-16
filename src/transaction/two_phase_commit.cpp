//
// Created by user on 23-7-16.
//

#include "transaction/two_phase_commit.h"

#include "epoch/epoch_manager.h"
#include "tools/utilities.h"

namespace Taas {
  // 事务发送到client初始化处理
  void TwoPC::ClientTxn_Init() {
    // txn_state_struct 记录当前事务的分片个数，完成个数
    tid_client = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->server_id());
    std::unique_ptr<TwoPCTxnStateStruct> txn_state_struct;
    txn_state_struct = std::make_unique<TwoPCTxnStateStruct>(sharding_num, 0, 0, 0, 0, 0, 0,
                                                             client_txn);  // 无用数据state
    txn_state_map.insert(tid_client, *txn_state_struct);
  }

  // 事务进行分片
  bool TwoPC::Sharding_2PL() {
    // 分片
    std::vector<std::unique_ptr<proto::Transaction>> sharding_row_vector;
    for (uint64_t i = 0; i < sharding_num; i++) {
      sharding_row_vector.emplace_back(std::make_unique<proto::Transaction>());
      sharding_row_vector[i]->set_csn(txn_ptr->csn());
      sharding_row_vector[i]->set_server_id(txn_ptr->server_id());
      sharding_row_vector[i]->set_client_ip(txn_ptr->client_ip());
      sharding_row_vector[i]->set_client_txn_id(txn_ptr->client_txn_id());
      sharding_row_vector[i]->set_sharding_id(i);
    }
    // 为创建的事务添加子事务行
    for (auto i = 0; i < txn_ptr->row_size(); i++) {
      const auto& row = txn_ptr->row(i);
      auto row_ptr = sharding_row_vector[GetHashValue(row.key())]->add_row();
      (*row_ptr) = row;
    }
    // 分片后发送到applicant
    for (uint64_t i = 0; i < sharding_num; i++) {
      Send(ctx, epoch, i, txn_ptr, proto::TxnType::RemoteServerTxn);
    }
  }

  // 2PL 上锁
  // map上锁方式之后写
  bool TwoPC::Two_PL_LOCK(proto::Transaction& txn) {
    // 上锁完成返回到coordinator
    tid = std::to_string(txn.csn()) + ":" + std::to_string(txn_ptr->server_id());
    uint64_t key_lock_num = 0;
    std::string tmp = "-1";
    for (auto iter = key_sorted.begin(); iter != key_sorted.end(); i++) {
      row_lock_map.getValue(iter->first, tmp);
      if (tmp == tid) {
        key_lock_num++;
      } else if (tmp == "-1" || tmp == "0") {
        row_lock_map.insert(iter->first, tid);
        key_lock_num++;
      } else {
        return false;
      }
    }
    return key_lock_num == key_sorted.size();
  }

  // 2PL 解锁
  bool TwoPC::Two_PL_UNLOCK(proto::Transaction& txn) {
    // 事务完全提交或中途abort调用，无需返回coordinator?
    uint64_t key_unlock_num = 0;
    std::string tmp = "-1";
    for (auto iter = key_sorted.begin(); iter != key_sorted.end(); i++) {
      row_lock_map.getValue(iter->first, tmp);
      if (tmp == tid) {
        row_lock_map.insert(iter->first, "0");
        key_unlock_num++;
      } else if (tmp == "-1" || tmp == "0") {
        key_unlock_num++;
      } else {
        return false;
      }
    }
    return key_unlock_num == key_sorted.size();
  }

  // 检查2PL阶段是否完成
  bool TwoPC::Check_2PL_complete(proto::Transaction& txn) {
    TwoPCTxnStateStruct txn_state_struct;
    txn_state_map.getValue(tid, txn_state_struct);
    return txn_state_struct.two_pl_num == txn_state_struct.txn_sharding_num;
  }

  // 检查2PC prepare是否完成
  bool TwoPC::Check_2PC_Prepare_complete(proto::Transaction& txn) {
    TwoPCTxnStateStruct txn_state_struct;
    txn_state_map.getValue(tid, txn_state_struct);
    return txn_state_struct.two_pc_prepare_num == txn_state_struct.txn_sharding_num;
  }

  // 检查2PC commit是否完成
  bool TwoPC::Check_2PL_Commit_complete(proto::Transaction& txn) {
    TwoPCTxnStateStruct txn_state_struct;
    txn_state_map.getValue(tid, txn_state_struct);
    return txn_state_struct.two_pc_commit_num == txn_state_struct.txn_sharding_num;
  }

  // 发送事务给指定applicant、coordinator
  // to_whom 为编号
  bool TwoPC::Send(const Context& ctx, uint64_t& epoch, uint64_t& to_whom, proto::Transaction& txn,
                   proto::TxnType txn_type) {
    // assert(to_whom != ctx.txn_node_ip_index);
    auto msg = std::make_unique<proto::Message>();
    auto* txn_temp = msg->mutable_txn();
    *(txn_temp) = txn;
    txn_temp->set_txn_type(txn_type);
    auto serialized_txn_str_ptr = std::make_unique<std::string>();
    Gzip(msg.get(), serialized_txn_str_ptr.get());

    assert(!serialized_txn_str_ptr->empty());
    // printf("send thread  message epoch %d server_id %lu type %d\n", 0, to_whom, txn_type);
    MessageQueue::send_to_server_queue->enqueue(std::make_unique<send_params>(
        to_whom, 0, "", epoch, txn_type, std::move(serialized_txn_str_ptr), nullptr));
    return MessageQueue::send_to_server_queue->enqueue(std::make_unique<send_params>(
        to_whom, 0, "", epoch, proto::TxnType::NullMark, nullptr, nullptr));
  }

  // 发送给client abort/commit
  bool TwoPC::SendToClient(const Context& ctx, proto::Transaction& txn, proto::TxnType txn_type,
                           proto::TxnState txn_state) {
    // commit/abort时发送
    // 不是本地事务不进行回复
    if (txn.server_id() != ctx.txn_node_ip_index) return true;

    // 设置txn的状态并创建proto对象
    txn.set_txn_state(txn_state);
    auto msg = std::make_unique<proto::Message>();
    auto rep = msg->mutable_reply_txn_result_to_client();
    rep->set_txn_type(txn_type);
    rep->set_txn_state(txn_state);
    rep->set_client_txn_id(txn.client_txn_id());

    // 将Transaction使用protobuf进行序列化，序列化的结果在serialized_txn_str_ptr中
    auto serialized_txn_str_ptr = std::make_unique<std::string>();
    Gzip(msg.get(), serialized_txn_str_ptr.get());

    // 将序列化的Transaction放到send_to_client_queue中，等待发送给client
    MessageQueue::send_to_client_queue->enqueue(std::make_unique<send_params>(
        txn.client_txn_id(), txn.csn(), txn.client_ip(), txn.commit_epoch(), txn_type,
        std::move(serialized_txn_str_ptr), nullptr));
    return MessageQueue::send_to_client_queue->enqueue(
        std::make_unique<send_params>(0, 0, "", 0, proto::TxnType::NullMark, nullptr, nullptr));
  }

  // 初始化哪些？
  bool TwoPC::Init(const Taas::Context& ctx_, uint64_t id) {
    ctx = ctx_;
    thread_id = id;
    sharding_num = ctx_.kTxnNodeNum;
    message_ptr = nullptr;
    txn_ptr = nullptr;
    return true;
  }

  // 处理接收到的消息
  bool TwoPC::HandleReceivedMessage() {
    sleep_flag = false;
    if (MessageQueue::listen_message_queue->try_dequeue(message_ptr)) {
      if (message_ptr->empty()) return false;
      message_string_ptr = std::make_unique<std::string>(
          static_cast<const char*>(message_ptr->data()), message_ptr->size());
      msg_ptr = std::make_unique<proto::Message>();
      res = UnGzip(msg_ptr.get(), message_string_ptr.get());
      assert(res);
      if (msg_ptr->type_case() == proto::Message::TypeCase::kTxn) {
        txn_ptr = std::make_unique<proto::Transaction>(*(msg_ptr->release_txn()));
        SetMessageRelatedCountersInfo();
        HandleReceivedTxn();
      } else {
        MessageQueue::request_queue->enqueue(std::move(msg_ptr));
        MessageQueue::request_queue->enqueue(nullptr);
      }
      sleep_flag = true;
    }
    return sleep_flag;
  }

  // 核心：根据txn type操作
  bool TwoPC::HandleReceivedTxn(std::unique_ptr<proto::Transaction> txn_ptr) {
    switch (txn_ptr->txn_type) {
      case proto::TxnType::ClientTxn: {
        ClientTxn_Init();
        Sharding_2PL();
        break;
      }
      case proto::TxnType::RemoteServerTxn: {
        GetKeySorted(txn_ptr);
        if (Two_PL_LOCK(txn_ptr, key_sorted)) {
          // 发送lock ok
          Send(ctx, epoch, ctx.txn_node_ip_index, txn_ptr, proto::TxnType::Lock_ok);
        } else {
          // 发送lock abort
          Send(ctx, epoch, ctx.txn_node_ip_index, txn_ptr, proto::TxnType::Lock_abort);
        }
        break;
      }
      case proto::TxnType::Lock_ok: {
        // 修改元数据
        TwoPCTxnStateStruct txn_state_struct;
        txn_state_map.getValue(tid, txn_state_struct);
        txn_state_struct.two_pl_reply++;
        txn_state_struct.two_pl_num++;

        // 所有应答收到
        if (txn_state_struct.two_pl_reply == txn_state_struct.txn_sharding_num) {
          if (Check_2PL_complete(txn_ptr)) {
            // 2pl完成，开始2pc prepare阶段
            for (uint64_t i = 0; i < sharding_num; i++) {
              Send(ctx, epoch, i, txn_ptr, proto::TxnType::Prepare_req);
            }
          } else {
            // 统一处理abort
            for (uint64_t i = 0; i < sharding_num; i++) {
              Send(ctx, epoch, i, txn_ptr, proto::TxnType::Abort_txn);
            }
            // 发送abort给client
            SendToClient(ctx, txn_ptr, proto::TxnType::Abort_txn, proto::TxnState::Abort);
          }
        }
        break;
      }
      case proto::TxnType::Lock_abort: {
        // 修改元数据
        TwoPCTxnStateStruct txn_state_struct;
        txn_state_map.getValue(tid, txn_state_struct);
        txn_state_struct.two_pl_reply++;
        // 直接发送abort
        for (uint64_t i = 0; i < sharding_num; i++) {
          Send(ctx, epoch, i, txn_ptr, proto::TxnType::Abort_txn);
        }
        // 发送abort给client
        SendToClient(ctx, txn_ptr, proto::TxnType::Abort_txn, proto::TxnState::Abort);
        break;
      }
      case proto::TxnType::Prepare_req: {
        // 日志操作等等，总之返回Prepare_ok
        Send(ctx, epoch, ctx.txn_node_ip_index, txn_ptr, proto::TxnType::Prepare_ok);
        break;
      }
      case proto::TxnType::Prepare_ok: {
        // 修改元数据
        TwoPCTxnStateStruct txn_state_struct;
        txn_state_map.getValue(tid, txn_state_struct);
        txn_state_struct.two_pc_prepare_reply++;
        txn_state_struct.two_pc_prepare_num++;
        // 当所有应答已经收到
        if (txn_state_struct.two_pc_prepare_reply == txn_state_struct.txn_sharding_num) {
          if (Check_2PC_Prepare_complete(txn_ptr)) {
            for (uint64_t i = 0; i < sharding_num; i++) {
              // Send(ctx, sharding_row_vector[i], proto::TxnType::Commit_req);
              Send(ctx, epoch, i, txn_ptr, proto::TxnType::Commit_req);
            }
          } else {
            // 统一处理abort
            for (uint64_t i = 0; i < sharding_num; i++) {
              // Send(ctx, sharding_row_vector[i], proto::TxnType::Abort_txn);
              Send(ctx, epoch, i, txn_ptr, proto::TxnType::Abort_txn);
            }
            // 发送abort给client
            SendToClient(ctx, txn_ptr, proto::TxnType::Abort_txn, proto::TxnState::Abort);
          }
        }
        break;
      }
      case proto::TxnType::Prepare_abort: {
        // 修改元数据
        TwoPCTxnStateStruct txn_state_struct;
        txn_state_map.getValue(tid, txn_state_struct);
        txn_state_struct.two_pc_prepare_reply++;
        // 直接发送abort
        for (uint64_t i = 0; i < sharding_num; i++) {
          // Send(ctx, sharding_row_vector[i], proto::TxnType::Abort_txn);
          Send(ctx, epoch, i, txn_ptr, proto::TxnType::Abort_txn);
        }
        // 发送abort给client
        SendToClient(ctx, txn_ptr, proto::TxnType::Abort_txn, proto::TxnState::Abort);
        break;
      }
      case proto::TxnType::Commit_req: {
        // 日志操作等等，总之返回Commit_ok
        Send(ctx, epoch, ctx.txn_node_ip_index, txn_ptr, proto::TxnType::Commit_ok);
        break;
      }
      case proto::TxnType::Commit_ok: {
        // 与上相同
        // 修改元数据
        TwoPCTxnStateStruct txn_state_struct;
        txn_state_map.getValue(tid, txn_state_struct);
        txn_state_struct.two_pc_commit_reply++;
        txn_state_struct.two_pc_commit_num++;
        // 当所有应答已经收到，并且commit阶段未完成
        if (txn_state_struct.two_pc_commit_reply == txn_state_struct.txn_sharding_num) {
          if (Check_2PL_Commit_complete(txn_ptr)) {
            SendToClient(ctx, txn_ptr, proto::TxnType::CommittedTxn, proto::TxnState::Commit);
          } else {
            // 统一处理abort
            for (uint64_t i = 0; i < sharding_num; i++) {
              Send(ctx, epoch, i, txn_ptr, proto::TxnType::Abort_txn);
            }
            SendToClient(ctx, txn_ptr, proto::TxnType::Abort_txn, proto::TxnState::Abort);
          }
        }
        break;
      }
      case proto::TxnType::Commit_abort: {
        // 与上相同
        // TwoPCTxnStateStruct txn_state_struct;
        // txn_state_map.getValue(tid, txn_state_struct);
        // txn_state_struct.two_pc_commit_reply++;
        // 直接发送abort
        for (uint64_t i = 0; i < sharding_num; i++) {
          Send(ctx, epoch, i, txn_ptr, proto::TxnType::Abort_txn);
        }
        // 发送abort给client
        SendToClient(ctx, txn_ptr, proto::TxnType::Abort_txn, proto::TxnState::Abort);
        break;
      }
      case proto::TxnType::Abort_txn: {
        Two_PL_UNLOCK(txn, key_sorted);
        break;
      }
      default:
        break;
    }

    SetMessageRelatedCountersInfo();

    return false;
  }

  // 修改哪些元数据？
  bool TwoPC::SetMessageRelatedCountersInfo() {
    // ？？？
    //        message_server_id = txn_ptr->server_id();
    //        txn_ptr->sharding_id();
    return true;
  }

}  // namespace Taas
