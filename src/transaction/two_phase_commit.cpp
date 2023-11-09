//
// Created by user on 23-7-16.
//

#include "transaction/two_phase_commit.h"

#include "epoch/epoch_manager.h"
#include "tools/utilities.h"
#include "proto/transaction.pb.h"
#include "storage/redo_loger.h"

namespace Taas {
    Context TwoPC::ctx;
    uint64_t TwoPC::sharding_num;
    std::atomic<uint64_t> TwoPC::successTxnNumber , TwoPC::totalTxnNumber ,
            TwoPC::failedTxnNumber ,TwoPC::lockFailed, TwoPC::validateFailed;
    concurrent_unordered_map<std::string, std::string> TwoPC::row_lock_map;
    concurrent_unordered_map<std::string, std::shared_ptr<TwoPCTxnStateStruct>> TwoPC::txn_state_map;
    std::mutex TwoPC::mutex;

    concurrent_unordered_map<std::string, std::string>
           TwoPC::row_map_csn;      /// tid, csn

    concurrent_unordered_map<std::string, std::string>
           TwoPC::row_map_data;

    // 事务发送到client初始化处理
  void TwoPC::ClientTxn_Init() {
    // txn_state_struct 记录当前事务的分片个数，完成个数
    totalTxnNumber.fetch_add(1);
    txn_ptr->set_csn(now_to_us());
    txn_ptr->set_server_id(ctx.taasContext.txn_node_ip_index);
    tid = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->server_id());
    txn_state_map.insert(tid, std::make_shared<Taas::TwoPCTxnStateStruct>(sharding_num, 0, 0, 0, 0, 0, 0,
                                                                          client_txn));
    if (totalTxnNumber % 1000 == 0) OUTPUTLOG("============= 2PC + 2PL INFO =============");

    }

  // 事务进行分片
  bool TwoPC::Sharding_2PL() {
//      LOG(INFO)<<"Sharding_2PL() -- tid : " << tid;
    // 分片
    std::vector<std::shared_ptr<proto::Transaction>> sharding_row_vector;
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
        Send(ctx, epoch, i, *sharding_row_vector[i], proto::TxnType::RemoteServerTxn);
//      Send(ctx, epoch, i, *txn_ptr, proto::TxnType::RemoteServerTxn);
    }
    return true;
  }

  // 2PL 上锁
  bool TwoPC::Two_PL_LOCK(proto::Transaction& txn) {
    // 上锁完成返回到coordinator
      GetKeySorted(txn);
//      auto tmp_tid = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->server_id());
//      auto tmp_keySorted = key_sorted;
    tid = std::to_string(txn.csn()) + ":" + std::to_string(txn.server_id());

    std::atomic<uint64_t> key_lock_num = 0;
//    auto tmp_tid = tid;
//      printSorted(1);
    for (auto iter = key_sorted.begin(); iter != key_sorted.end(); iter++) {
        /// read needs lock
        if (row_lock_map.try_lock(iter->first, tid)){
            key_lock_num.fetch_add(1);
        } else {
            lockFailed.fetch_add(1);
//            LOG(INFO)<< "[Can't Lock] key : "<< iter->first  << "  already locked by other txn , current txn: " << tid;
            Two_PL_UNLOCK(txn);     // unlock local first, then send message
            return false;
        }
    }
    // TODO: Validate readset
    if (!ValidateReadSet(txn)){
    //          LOG(INFO) <<"Txn ValidateReadSet check failed";
       validateFailed.fetch_add(1);
       Two_PL_UNLOCK(txn);
       return false;
    }
//      printSorted();
    return key_lock_num.load() == key_sorted.size();
  }

  // DeadLock
  bool TwoPC::Two_PL_LOCK_WAIT(proto::Transaction& txn){

      // 上锁完成返回到coordinator
      GetKeySorted(txn);
      tid = std::to_string(txn.csn()) + ":" + std::to_string(txn.server_id());
      std::atomic<uint64_t> key_lock_num = 0;

      for (auto iter = key_sorted.begin(); iter != key_sorted.end(); iter++) {
//          while (!row_lock_map.try_lock(iter->first, tid)){ } /// spinlock never block
          while (true) {
              if (!row_lock_map.try_lock(iter->first, tid)) {
                  printSorted(1);
              }
              else{
                  break;
              }
          }
          key_lock_num.fetch_add(1);
      }
      printSorted(1);
      if (!ValidateReadSet(txn)){
//          LOG(INFO) <<"Txn ValidateReadSet check failed";
          validateFailed.fetch_add(1);
          Two_PL_UNLOCK(txn);     // unlock local first, then send message
          return false;
      }
      return key_lock_num.load() == key_sorted.size();
  }

  // 2PL 解锁
  bool TwoPC::Two_PL_UNLOCK(proto::Transaction& txn) {
      GetKeySorted(txn);
      auto tmp_tid = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->server_id());
      auto tmp_keySorted = key_sorted;
    // 事务完全提交或中途abort调用，无需返回coordinator?
      tid = std::to_string(txn.csn()) + ":" + std::to_string(txn.server_id());
//      printSorted(2);
    for (auto iter = key_sorted.begin(); iter != key_sorted.end(); iter++) {
        auto tmp = row_lock_map.unlock(iter->first, tid);
        if (tmp != "") {
            std::shared_ptr<TwoPCTxnStateStruct> txn_state_struct;
            txn_state_map.getValue(tmp, txn_state_struct);
//            LOG(INFO)<< "[Can't Unlock]key : "<< iter->first  << " already locked by txn : " << tmp <<" , it's state is " << txn_state_struct->txn_state;
        }
    }
//    printSorted(2);
    return true;
  }

  // 检查2PL阶段是否完成
  bool TwoPC::Check_2PL_complete(proto::Transaction& txn, std::shared_ptr<TwoPCTxnStateStruct> txn_state_struct) {
//    std::shared_ptr<TwoPCTxnStateStruct> txn_state_struct;
//    txn_state_map.getValue(tid, txn_state_struct);
    return txn_state_struct->two_pl_num.load() == txn_state_struct->txn_sharding_num;
  }

  // 检查2PC prepare是否完成
  bool TwoPC::Check_2PC_Prepare_complete(proto::Transaction& txn, std::shared_ptr<TwoPCTxnStateStruct> txn_state_struct) {
//      std::shared_ptr<TwoPCTxnStateStruct> txn_state_struct;
//      txn_state_map.getValue(tid, txn_state_struct);
    return txn_state_struct->two_pc_prepare_num.load() == txn_state_struct->txn_sharding_num;
  }

  // 检查2PC commit是否完成
  bool TwoPC::Check_2PC_Commit_complete(proto::Transaction& txn, std::shared_ptr<TwoPCTxnStateStruct> txn_state_struct) {
//      std::shared_ptr<TwoPCTxnStateStruct> txn_state_struct;
//      txn_state_map.getValue(tid, txn_state_struct);
    return txn_state_struct->two_pc_commit_num.load() == txn_state_struct->txn_sharding_num;
  }

  // 发送事务给指定applicant、coordinator
  // to_whom 为编号
  bool TwoPC::Send(const Context& ctx, uint64_t& epoch, uint64_t& to_whom, proto::Transaction& txn,
                   proto::TxnType txn_type) {
    // assert(to_whom != ctx.txn_node_ip_index);
    if (to_whom == ctx.taasContext.txn_node_ip_index){
        auto msg = std::make_unique<proto::Message>();
        auto* txn_temp = msg->mutable_txn();
        *(txn_temp) = txn;
        txn_temp->set_txn_type(txn_type);
        auto serialized_txn_str = std::string();
        Gzip(msg.get(), &serialized_txn_str);
        void *data = static_cast<void *>(const_cast<char *>(serialized_txn_str.data()));
        MessageQueue::listen_message_txn_queue->enqueue(
                std::make_unique<zmq::message_t>(data, serialized_txn_str.size()));
        return true;
    }
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
      if (txn_state == proto::TxnState::Commit){
          successTxnNumber.fetch_add(1);
      } else {
          failedTxnNumber.fetch_add(1);
      }

//      if (totalTxnNumber % ctx.taasContext.print_mode_size == 0) OUTPUTLOG("============= 2PC + 2PL INFO =============");


      // only coordinator can send to client
    if (txn.server_id() != ctx.taasContext.txn_node_ip_index) return true;

    // 设置txn的状态并创建proto对象
    txn.set_txn_state(txn_state);
    auto msg = std::make_unique<proto::Message>();
    auto rep = msg->mutable_reply_txn_result_to_client();
//    rep->set_txn_type(txn_type);
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

  // static 初始化哪些
  bool TwoPC::Init(const Taas::Context& ctx_, uint64_t id) {
    ctx = ctx_;
    sharding_num = ctx.taasContext.kTxnNodeNum;
    successTxnNumber.store(0);
    totalTxnNumber.store(0);
    failedTxnNumber.store(0);
    lockFailed.store(0);
    validateFailed.store(0);

    // static init
    return true;
  }

  // 处理接收到的消息
  // change queue to listen_message_epoch_queue
  bool TwoPC::HandleClientMessage() {
      while(!EpochManager::IsTimerStop()) {
          MessageQueue::listen_message_txn_queue->wait_dequeue(message_ptr);
          if (message_ptr == nullptr || message_ptr->empty()) continue;
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
      }
      return true;
  }

  // 处理接收到的消息 from server
  // change queue to listen_message_epoch_queue
  bool TwoPC::HandleReceivedMessage() {
    while(!EpochManager::IsTimerStop()) {
        MessageQueue::listen_message_epoch_queue->wait_dequeue(message_ptr);
        if (message_ptr == nullptr || message_ptr->empty()) continue;
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
    }
    return true;
  }


    // 核心：根据txn type操作
  bool TwoPC::HandleReceivedTxn() {
      if (txn_ptr->txn_type() == proto::TxnType::ClientTxn) {
          ClientTxn_Init();
          Sharding_2PL();
          return true;
      }

//      auto tmp_type = txn_ptr->txn_type();
//      auto tmp_tid = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->server_id());
//      GetKeySorted(*txn_ptr);
//      auto tmp_keySorted = key_sorted;

    switch (txn_ptr->txn_type()) {
      case proto::TxnType::ClientTxn: {
        ClientTxn_Init();
        Sharding_2PL();
        break;
      }
      case proto::TxnType::RemoteServerTxn: {
        if (Two_PL_LOCK(*txn_ptr)) {
//          if (Two_PL_LOCK_WAIT(*txn_ptr)) {
          // 发送lock ok
          Send(ctx, epoch, ctx.taasContext.txn_node_ip_index, *txn_ptr, proto::TxnType::Lock_ok);
        } else {
          // 发送lock abort
          Send(ctx, epoch, ctx.taasContext.txn_node_ip_index, *txn_ptr, proto::TxnType::Lock_abort);
        }
        break;
      }
      case proto::TxnType::Lock_ok: {
        // 修改元数据
        tid = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->server_id());
        std::shared_ptr<TwoPCTxnStateStruct> txn_state_struct;
        txn_state_map.getValue(tid, txn_state_struct);
        if(txn_state_struct == nullptr) txn_state_struct = std::make_shared<TwoPCTxnStateStruct>();

        txn_state_struct->two_pl_reply.fetch_add(1);
        txn_state_struct->two_pl_num.fetch_add(1);

        // 所有应答收到
        if (txn_state_struct->two_pl_reply.load() == txn_state_struct->txn_sharding_num) {
            if (Check_2PL_complete(*txn_ptr, txn_state_struct)) {
            // 2pl完成，开始2pc prepare阶段
            for (uint64_t i = 0; i < sharding_num; i++) {
              Send(ctx, epoch, i, *txn_ptr, proto::TxnType::Prepare_req);
            }
          } else {
            // 统一处理abort
            for (uint64_t i = 0; i < sharding_num; i++) {
                // the unlock request is handled by other threads
              Send(ctx, epoch, i, *txn_ptr, proto::TxnType::Abort_txn);
            }
            // 发送abort给client
            SendToClient(ctx, *txn_ptr, proto::TxnType::Abort_txn, proto::TxnState::Abort);
          }
        }
        break;
      }
      case proto::TxnType::Lock_abort: {
        // 修改元数据, no need
//        TwoPCTxnStateStruct txn_state_struct;
//        txn_state_map.getValue(tid, txn_state_struct);
//        txn_state_struct.two_pl_reply++;
        // 直接发送abort
        for (uint64_t i = 0; i < sharding_num; i++) {
          Send(ctx, epoch, i, *txn_ptr, proto::TxnType::Abort_txn);
        }
        // 发送abort给client
        SendToClient(ctx, *txn_ptr, proto::TxnType::Abort_txn, proto::TxnState::Abort);
        break;
      }
      case proto::TxnType::Prepare_req: {
        // 日志操作等等，总之返回Prepare_ok
        Send(ctx, epoch, ctx.taasContext.txn_node_ip_index, *txn_ptr, proto::TxnType::Prepare_ok);
        break;
      }
      case proto::TxnType::Prepare_ok: {
        // 修改元数据
        tid = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->server_id());
        std::shared_ptr<TwoPCTxnStateStruct> txn_state_struct;
        txn_state_map.getValue(tid, txn_state_struct);
        if(txn_state_struct == nullptr) txn_state_struct = std::make_shared<TwoPCTxnStateStruct>();

        txn_state_struct->two_pc_prepare_reply.fetch_add(1);
        txn_state_struct->two_pc_prepare_num.fetch_add(1);
        // 当所有应答已经收到
        if (txn_state_struct->two_pc_prepare_reply.load() == txn_state_struct->txn_sharding_num) {
          if (Check_2PC_Prepare_complete(*txn_ptr, txn_state_struct)) {
            for (uint64_t i = 0; i < sharding_num; i++) {
              // Send(ctx, sharding_row_vector[i], proto::TxnType::Commit_req);
              Send(ctx, epoch, i, *txn_ptr, proto::TxnType::Commit_req);
            }
          } else {
            // 统一处理abort
            for (uint64_t i = 0; i < sharding_num; i++) {
              // Send(ctx, sharding_row_vector[i], proto::TxnType::Abort_txn);
              Send(ctx, epoch, i, *txn_ptr, proto::TxnType::Abort_txn);
            }
            // 发送abort给client
            SendToClient(ctx, *txn_ptr, proto::TxnType::Abort_txn, proto::TxnState::Abort);
          }
        }
        break;
      }
      case proto::TxnType::Prepare_abort: {
        // 修改元数据, no need
        // 直接发送abort
        for (uint64_t i = 0; i < sharding_num; i++) {
          // Send(ctx, sharding_row_vector[i], proto::TxnType::Abort_txn);
          Send(ctx, epoch, i, *txn_ptr, proto::TxnType::Abort_txn);
        }
        // 发送abort给client
        SendToClient(ctx, *txn_ptr, proto::TxnType::Abort_txn, proto::TxnState::Abort);
        break;
      }
      case proto::TxnType::Commit_req: {
        // 日志操作等等，总之返回Commit_ok
        Send(ctx, epoch, ctx.taasContext.txn_node_ip_index, *txn_ptr, proto::TxnType::Commit_ok);
        break;
      }
      case proto::TxnType::Commit_ok: {
        // 与上相同
        // 修改元数据
        tid = std::to_string(txn_ptr->csn()) + ":" + std::to_string(txn_ptr->server_id());
//        std::shared_ptr<TwoPCTxnStateStruct> txn_state_struct;
//        txn_state_map.getValue(tid, txn_state_struct);
        std::shared_ptr<TwoPCTxnStateStruct> txn_state_struct;
        txn_state_map.getValue(tid, txn_state_struct);
        if(txn_state_struct == nullptr) txn_state_struct = std::make_shared<TwoPCTxnStateStruct>();

        txn_state_struct->two_pc_commit_reply.fetch_add(1);
        txn_state_struct->two_pc_commit_num.fetch_add(1);

        // 当所有应答已经收到，并且commit阶段未完成
        if (txn_state_struct->two_pc_commit_reply.load() == txn_state_struct->txn_sharding_num) {
            if (Check_2PC_Commit_complete(*txn_ptr, txn_state_struct)) {
                for (uint64_t i = 0; i < sharding_num; i++) {
                  // 解锁 use Abort_xtn type to unlock
                  Send(ctx, epoch, i, *txn_ptr, proto::TxnType::Abort_txn);
                }
                txn_state_struct->txn_state = commit_done;
                SendToClient(ctx, *txn_ptr, proto::TxnType::CommittedTxn, proto::TxnState::Commit);
                if(txn_ptr->server_id() == ctx.taasContext.txn_node_ip_index) {
                    // TODO: refering to RedoLoger::RedoLog(txn_ptr);
                    txn_ptr->set_commit_epoch(EpochManager::GetPushDownEpoch());
                    RedoLoger::RedoLog(txn_ptr);
                }
                // TODO: ValidateReadSet
                UpdateReadSet(*txn_ptr);
        } else {
            // 统一处理abort
            for (uint64_t i = 0; i < sharding_num; i++) {
              Send(ctx, epoch, i, *txn_ptr, proto::TxnType::Abort_txn);
            }
            SendToClient(ctx, *txn_ptr, proto::TxnType::Abort_txn, proto::TxnState::Abort);
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
          Send(ctx, epoch, i, *txn_ptr, proto::TxnType::Abort_txn);
        }
        // 发送abort给client
        SendToClient(ctx, *txn_ptr, proto::TxnType::Abort_txn, proto::TxnState::Abort);
        break;
      }
      case proto::TxnType::Abort_txn: {
          Two_PL_UNLOCK(*txn_ptr);
        break;
      }
      default:
        break;
    }

    SetMessageRelatedCountersInfo();

    return true;
  }

  // 修改哪些元数据
  bool TwoPC::SetMessageRelatedCountersInfo() {
    message_server_id = txn_ptr->server_id();
    txn_ptr->sharding_id();
    return true;
  }

  void TwoPC::OUTPUTLOG(const std::string& s){
      LOG(INFO) << s.c_str() <<
                "\ntotalTxnNumber: " << totalTxnNumber << "\t\tfailedTxnNumber: " << failedTxnNumber<<"\t\tsuccessTxnNumber: " << successTxnNumber <<
                "\nlockFailed: " << lockFailed << "\t\tvalidateFailed: " << validateFailed << "\t\ttime:" << now_to_us()<<
                "\n************************************************ ";

  }

}  // namespace Taas
