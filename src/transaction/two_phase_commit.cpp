//
// Created by user on 23-7-16.
//

#include "transaction/two_phase_commit.h"
#include "epoch/epoch_manager.h"
#include "tools/utilities.h"

namespace Taas {
    bool TwoPC::Init(uint64_t id) {
//        ctx = ctx_;
//        thread_id = id;
//        sharding_num = ctx_.kTxnNodeNum;
//        message_ptr = nullptr;
//        txn_ptr.reset();
        return false;
    }

    bool TwoPC::HandleReceivedMessage() {
//        while(!EpochManager::IsTimerStop()) {
//            MessageQueue::listen_message_queue->wait_dequeue(message_ptr);
//            if (message_ptr->empty()) continue;
//            message_string_ptr = std::make_unique<std::string>(static_cast<const char *>(message_ptr->data()),
//                                                               message_ptr->size());
//            msg_ptr = std::make_unique<proto::Message>();
//            res = UnGzip(msg_ptr.get(), message_string_ptr.get());
//            assert(res);
//            if (msg_ptr->type_case() == proto::Message::TypeCase::kTxn) {
//                txn_ptr = std::make_unique<proto::Transaction>(*(msg_ptr->release_txn()));
//                SetMessageRelatedCountersInfo();
//                HandleReceivedTxn();
//            } else {
//                MessageQueue::request_queue->enqueue(std::move(msg_ptr));
//                MessageQueue::request_queue->enqueue(nullptr);
//            }
//        }
//        return false;
        return false;
    }

    bool TwoPC::HandleReceivedTxn() {
//        if(txn_ptr->txn_type() == proto::TxnType::ClientTxn) {
//            txn_ptr->set_csn(now_to_us());
//            txn_ptr->set_server_id(ctx.txn_node_ip_index);
//        }
        SetMessageRelatedCountersInfo();



        return false;
    }

    bool TwoPC::SetMessageRelatedCountersInfo() {
//        message_server_id = txn_ptr->server_id();
//        txn_ptr->sharding_id();
        return true;
    }

}

