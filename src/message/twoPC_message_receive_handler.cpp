//
// Created by zwx on 23-7-17.
//

#include "message/twoPC_message_receive_handler.h"
//
// Created by 周慰星 on 11/9/22.
//
#include <queue>
#include <utility>

#include "epoch/epoch_manager.h"
#include "message/message.h"
#include "message/epoch_message_receive_handler.h"
#include "tools/utilities.h"
#include "transaction/merge.h"
#include "message/twoPC_message_send_handler.h"


namespace Taas {
//    const uint64_t PACKNUM = 1L<<32;///

    std::vector<uint64_t>
            TwoPCMessageReceiveHandler::shard_send_ack_epoch_num,
            TwoPCMessageReceiveHandler::backup_send_ack_epoch_num,
            TwoPCMessageReceiveHandler::backup_insert_set_send_ack_epoch_num,
            TwoPCMessageReceiveHandler::abort_set_send_ack_epoch_num; /// check and reply ack

    bool TwoPCMessageReceiveHandler::Init(const Context& ctx_, uint64_t id) {
        message_ptr = nullptr;
        txn_ptr.reset();
        thread_id = id;

//        max_length = ctx_.kCacheMaxLength;
        shard_num = ctx_.taasContext.kTxnNodeNum;

        return true;
    }

    bool TwoPCMessageReceiveHandler::StaticInit() {
        auto max_length = TaasContext::kCacheMaxLength;
        auto shard_num = TaasContext::kTxnNodeNum;

        shard_send_ack_epoch_num.resize(shard_num + 1);
        backup_send_ack_epoch_num.resize(shard_num + 1);
        backup_insert_set_send_ack_epoch_num.resize(shard_num + 1);
        abort_set_send_ack_epoch_num.resize(shard_num + 1);
        for(int i = 0; i <= (int) shard_num; i ++ ) { /// start at 1, not 0
            shard_send_ack_epoch_num[i] = 1;
            backup_send_ack_epoch_num[i] = 1;
            backup_insert_set_send_ack_epoch_num[i] = 1;
            abort_set_send_ack_epoch_num[i] = 1;
        }
        epoch_backup_txn.resize(max_length);
        epoch_insert_set.resize(max_length);
        epoch_abort_set.resize(max_length);

        return true;
    }

    void TwoPCMessageReceiveHandler::HandleReceivedMessage() {
        while(!EpochManager::IsTimerStop()) {
            if(MessageQueue::listen_message_queue->try_dequeue(message_ptr)) {
                if (message_ptr->empty()) continue;
                message_string_ptr = std::make_unique<std::string>(static_cast<const char *>(message_ptr->data()),
                                                                   message_ptr->size());
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
            else {
                usleep(50);
            }
        }
    }

    bool TwoPCMessageReceiveHandler::SetMessageRelatedCountersInfo() {
        message_server_id = txn_ptr->txn_server_id();
        txn_ptr->shard_id();
        return true;
    }

    bool TwoPCMessageReceiveHandler::HandleReceivedTxn() {
        if(txn_ptr->txn_type() == proto::TxnType::ClientTxn) {
            txn_ptr->set_commit_epoch(EpochManager::GetPhysicalEpoch());
            txn_ptr->set_csn(now_to_us());
            txn_ptr->set_txn_server_id(TaasContext::txn_node_ip_index);
        }
        SetMessageRelatedCountersInfo();
        switch (txn_ptr->txn_type()) {
            ///这里需要注意 这几个计数器是以server_id为粒度增加的，不是线程id ！！！
            case proto::TxnType::ClientTxn : {/// sql node --> txn node
                HandleClientTxn();
                break;
            }
            case proto::TxnType::RemoteServerTxn :
            case proto::TxnType::BackUpTxn :
            case proto::EpochShardEndFlag:
            case proto::EpochRemoteServerEndFlag:
            case proto::EpochBackUpEndFlag:
            case proto::EpochCommittedTxnEndFlag:
            case proto::TxnType::AbortSet :
            case proto::TxnType::InsertSet :
            case proto::TxnType::EpochShardACK :
            case proto::TxnType::BackUpACK :
            case proto::TxnType::AbortSetACK :
            case proto::TxnType::InsertSetACK :
            case proto::TxnType::EpochLogPushDownComplete :
            case proto::NullMark :
            case proto::TxnType_INT_MIN_SENTINEL_DO_NOT_USE_ :
            case proto::TxnType_INT_MAX_SENTINEL_DO_NOT_USE_ :
            case proto::CommittedTxn :
            case proto::Lock_ok :
            case proto::Lock_abort :
            case proto::Prepare_req :
            case proto::Prepare_ok :
            case proto::Prepare_abort :
            case proto::Commit_req :
            case proto::Commit_ok :
            case proto::Commit_abort :
            case proto::Abort_txn :
            case proto::ShardedClientTxn:
            case proto::EpochRemoteServerACK:
                break;
        }
        return true;
    }

    bool TwoPCMessageReceiveHandler::HandleClientTxn() {
        std::vector<std::shared_ptr<proto::Transaction>> shard_row_vector;
        for(uint64_t i = 0; i < shard_num; i ++) {
            shard_row_vector.emplace_back(std::make_shared<proto::Transaction>());
            shard_row_vector[i]->set_csn(txn_ptr->csn());
            shard_row_vector[i]->set_commit_epoch(txn_ptr->commit_epoch());
            shard_row_vector[i]->set_txn_server_id(txn_ptr->txn_server_id());
            shard_row_vector[i]->set_client_ip(txn_ptr->client_ip());
            shard_row_vector[i]->set_client_txn_id(txn_ptr->client_txn_id());
            shard_row_vector[i]->set_shard_id(i);
        }
        std::unordered_map<std::string, std::unique_ptr<proto::Row>> rows;
        for(auto &i : txn_ptr->row()) {///sort the keys, to avoid dead lock
            rows[i.key()] = std::make_unique<proto::Row>(i);
        }
        for(auto &i : rows) {
            auto row_ptr = shard_row_vector[GetHashValue(i.second->key())]->add_row();
            (*row_ptr) = *(i.second);
        }
        uint64_t txn_shard_num = 0;
        for(uint64_t i = 0; i < shard_num; i ++) {
            if(shard_row_vector[i]->row_size() > 0) {
                txn_shard_num |= 1<<i;
                ///shard sending
                if(i == TaasContext::txn_node_ip_index) {
                    continue;
                }
                else {
//                    TwoPCMessageSendHandler::SendTxnToServer(i, shard_row_vector[i], proto::TxnType::);
                }
            }
        }
        auto txn_state = std::make_unique<TwoPCTxnStateStruct>();
        if(shard_row_vector[TaasContext::txn_node_ip_index]->row_size() > 0) {
            ///read version check need to wait until last epoch has committed.

        }

        return true;
    }
    
}