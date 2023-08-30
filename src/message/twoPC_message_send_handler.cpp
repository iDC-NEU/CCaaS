//
// Created by zwx on 23-7-17.
//

#include "message/twoPC_message_send_handler.h"
#include "message/twoPC_message_receive_handler.h"
#include "tools/utilities.h"
#include "transaction/merge.h"

namespace Taas {
    std::atomic<uint64_t> TwoPCMessageSendHandler::TotalLatency(0), TwoPCMessageSendHandler::TotalTxnNum(0),
            TwoPCMessageSendHandler::TotalSuccessTxnNUm(0), TwoPCMessageSendHandler::TotalSuccessLatency(0);
    std::vector<std::unique_ptr<std::atomic<uint64_t>>> TwoPCMessageSendHandler::sharding_send_epoch,
            TwoPCMessageSendHandler::backup_send_epoch,
            TwoPCMessageSendHandler::abort_set_send_epoch,
            TwoPCMessageSendHandler::insert_set_send_epoch;

    uint64_t TwoPCMessageSendHandler::sharding_sent_epoch = 1, TwoPCMessageSendHandler::backup_sent_epoch = 1,
            TwoPCMessageSendHandler::abort_sent_epoch = 1,
            TwoPCMessageSendHandler::insert_set_sent_epoch = 1, TwoPCMessageSendHandler::abort_set_sent_epoch = 1;

    void TwoPCMessageSendHandler::StaticInit(const Context& ctx) {
        sharding_send_epoch.resize(ctx.taasContext.kTxnNodeNum);
        backup_send_epoch.resize(ctx.taasContext.kTxnNodeNum);
        abort_set_send_epoch.resize(ctx.taasContext.kTxnNodeNum);
        insert_set_send_epoch.resize(ctx.taasContext.kTxnNodeNum);
        for(uint64_t i = 0; i < ctx.taasContext.kTxnNodeNum; i ++) {
            backup_send_epoch [i] = std::make_unique<std::atomic<uint64_t>>(1);
            abort_set_send_epoch [i] = std::make_unique<std::atomic<uint64_t>>(1);
            sharding_send_epoch[i] = std::make_unique<std::atomic<uint64_t>>(1);
            insert_set_send_epoch[i] = std::make_unique<std::atomic<uint64_t>>(1);
        }
    }

    void TwoPCMessageSendHandler::StaticClear() {
    }

/**
 * @brief 将txn设置事务状态，并通过protobuf将Reply序列化，将序列化的结果放到send_to_client_queue中，等待发送给客户端
 *
 * @param ctx XML文件的配置信息
 * @param txn 等待回复给client的事务
 * @param txn_state 告诉client此txn的状态(Success or Abort)
 */
    bool TwoPCMessageSendHandler::SendTxnCommitResultToClient(const Context &ctx, std::shared_ptr<proto::Transaction> txn_ptr, proto::TxnState txn_state) {
        if(txn_ptr->server_id() != ctx.taasContext.txn_node_ip_index) return true;

        txn_ptr->set_txn_state(txn_state);
        auto msg = std::make_unique<proto::Message>();
        auto rep = msg->mutable_reply_txn_result_to_client();
        rep->set_txn_state(txn_state);
        rep->set_client_txn_id(txn_ptr->client_txn_id());

        auto serialized_txn_str_ptr = std::make_unique<std::string>();
        Gzip(msg.get(), serialized_txn_str_ptr.get());
        auto tim = now_to_us() - txn_ptr->csn();
        TotalLatency.fetch_add(tim);
        TotalTxnNum.fetch_add(1);
        if(txn_state == proto::TxnState::Commit) {
            TotalSuccessLatency.fetch_add(tim);
            TotalSuccessTxnNUm.fetch_add(1);
        }
        MessageQueue::send_to_client_queue->enqueue(std::make_unique<send_params>(txn_ptr->client_txn_id(), txn_ptr->csn(), txn_ptr->client_ip(), txn_ptr->commit_epoch(), proto::TxnType::CommittedTxn, std::move(serialized_txn_str_ptr), nullptr));
        return MessageQueue::send_to_client_queue->enqueue(std::make_unique<send_params>(0, 0, "", 0, proto::TxnType::NullMark, nullptr, nullptr));
    }

    bool TwoPCMessageSendHandler::SendTxnToServer(const Context& ctx, uint64_t &to_whom, std::shared_ptr<proto::Transaction> txn_ptr, proto::TxnType txn_type) {
        auto pack_param = std::make_unique<pack_params>(to_whom, 0, "", 0, txn_type, nullptr);
        switch (txn_type) {
            case proto::TxnType::RemoteServerTxn : {
                return SendRemoteServerTxn(ctx, to_whom, txn_ptr, txn_type);
            }
            case proto::TxnType::BackUpTxn :
            case proto::TxnType::BackUpACK :
            case proto::TxnType::AbortSetACK :
            case proto::TxnType::InsertSetACK :
            case proto::TxnType::EpochShardingACK :
            case  proto::TxnType::EpochLogPushDownComplete :
            case proto::NullMark:
            case proto::TxnType_INT_MIN_SENTINEL_DO_NOT_USE_:
            case proto::TxnType_INT_MAX_SENTINEL_DO_NOT_USE_:
            case proto::ClientTxn:
            case proto::EpochEndFlag:
            case proto::CommittedTxn:
            case proto::BackUpEpochEndFlag:
            case proto::AbortSet:
            case proto::InsertSet:
                break;
        }
        return true;
    }

    bool TwoPCMessageSendHandler::SendRemoteServerTxn(const Context& ctx, uint64_t& to_whom, std::shared_ptr<proto::Transaction> txn_ptr, proto::TxnType txn_type) {
        auto msg = std::make_unique<proto::Message>();
        auto* txn_temp = msg->mutable_txn();
        *(txn_temp) = *txn_ptr;
        txn_temp->set_txn_type(txn_type);
        auto serialized_txn_str_ptr = std::make_unique<std::string>();
        Gzip(msg.get(), serialized_txn_str_ptr.get());
        assert(!serialized_txn_str_ptr->empty());
        if(ctx.taasContext.taas_mode == TaasMode::MultiMaster) {
            for (uint64_t i = 0; i < ctx.taasContext.kTxnNodeNum; i++) {
                if (i == ctx.taasContext.txn_node_ip_index) continue;/// send to everyone
                auto str_copy = std::make_unique<std::string>(*serialized_txn_str_ptr);
                MessageQueue::send_to_server_queue->enqueue(std::make_unique<send_params>(i, 0, "", 0, txn_type, std::move(str_copy), nullptr));
            }
        }
        else {
            MessageQueue::send_to_server_queue->enqueue( std::make_unique<send_params>(to_whom, 0, "",0, txn_type, std::move(serialized_txn_str_ptr), nullptr));
        }
        return MessageQueue::send_to_server_queue->enqueue( std::make_unique<send_params>(0, 0, "",0, proto::TxnType::NullMark,
                                                                                          nullptr, nullptr));
    }

    bool TwoPCMessageSendHandler::SendACK(const Context &ctx, uint64_t &epoch, uint64_t &to_whom, proto::TxnType txn_type) {
        if(to_whom == ctx.taasContext.txn_node_ip_index) return true;
        auto msg = std::make_unique<proto::Message>();
        auto* txn_end = msg->mutable_txn();
        txn_end->set_server_id(ctx.taasContext.txn_node_ip_index);
        txn_end->set_txn_type(txn_type);
        txn_end->set_commit_epoch(epoch);
        txn_end->set_sharding_id(0);
        std::vector<std::string> keys, values;
        auto serialized_txn_str_ptr = std::make_unique<std::string>();
        Gzip(msg.get(), serialized_txn_str_ptr.get());
        MessageQueue::send_to_server_queue->enqueue(std::make_unique<send_params>(to_whom, 0, "", 0, txn_type, std::move(serialized_txn_str_ptr), nullptr));
        return MessageQueue::send_to_server_queue->enqueue(std::make_unique<send_params>(0, 0, "", 0, proto::TxnType::NullMark, nullptr, nullptr));
    }

    bool TwoPCMessageSendHandler::SendMessageToAll(const Context &ctx, proto::TxnType txn_type) {
        auto msg = std::make_unique<proto::Message>();
        auto* txn_end = msg->mutable_txn();
        txn_end->set_server_id(ctx.taasContext.txn_node_ip_index);
        txn_end->set_txn_type(txn_type);
        txn_end->set_sharding_id(0);
        auto serialized_txn_str_ptr = std::make_unique<std::string>();
        Gzip(msg.get(), serialized_txn_str_ptr.get());
        for (uint64_t i = 0; i < ctx.taasContext.kTxnNodeNum; i++) {
            if (i == ctx.taasContext.txn_node_ip_index) continue;/// send to everyone
            auto str_copy = std::make_unique<std::string>(*serialized_txn_str_ptr);
            MessageQueue::send_to_server_queue->enqueue(std::make_unique<send_params>(i, 0, "", 0, txn_type, std::move(str_copy), nullptr));
        }
        return MessageQueue::send_to_server_queue->enqueue(std::make_unique<send_params>(0, 0, "", 0, proto::TxnType::NullMark, nullptr, nullptr));
    }

}