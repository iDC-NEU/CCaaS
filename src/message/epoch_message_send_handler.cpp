//
// Created by 周慰星 on 11/9/22.
//
#include "message/epoch_message_send_handler.h"
#include "message/epoch_message_receive_handler.h"
#include "tools/utilities.h"
#include "transaction/merge.h"

namespace Taas {

    Context EpochMessageSendHandler::ctx;
    std::atomic<uint64_t> EpochMessageSendHandler::TotalLatency(0), EpochMessageSendHandler::TotalTxnNum(0),
            EpochMessageSendHandler::TotalSuccessTxnNUm(0), EpochMessageSendHandler::TotalSuccessLatency(0);
    std::vector<std::unique_ptr<std::atomic<uint64_t>>> EpochMessageSendHandler::sharding_send_epoch,
            EpochMessageSendHandler::backup_send_epoch,
            EpochMessageSendHandler::abort_set_send_epoch,
            EpochMessageSendHandler::insert_set_send_epoch;

    uint64_t EpochMessageSendHandler::sharding_sent_epoch = 1, EpochMessageSendHandler::backup_sent_epoch = 1,
            EpochMessageSendHandler::abort_sent_epoch = 1,
            EpochMessageSendHandler::insert_set_sent_epoch = 1, EpochMessageSendHandler::abort_set_sent_epoch = 1;

    void EpochMessageSendHandler::StaticInit(const Context& _ctx) {
        ctx = _ctx;
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

    void EpochMessageSendHandler::StaticClear() {
    }

/**
 * @brief 将txn设置事务状态，并通过protobuf将Reply序列化，将序列化的结果放到send_to_client_queue中，等待发送给客户端
 *
 * @param ctx XML文件的配置信息
 * @param txn 等待回复给client的事务
 * @param txn_state 告诉client此txn的状态(Success or Abort)
 */
bool EpochMessageSendHandler::SendTxnCommitResultToClient(const std::shared_ptr<proto::Transaction>& txn_ptr, proto::TxnState txn_state) {
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

    bool EpochMessageSendHandler::SendTxnToServer(uint64_t &epoch, uint64_t &to_whom, const std::shared_ptr<proto::Transaction>& txn_ptr, proto::TxnType txn_type) {
        if(ctx.taasContext.kTxnNodeNum > 1) {
            auto pack_param = std::make_unique<pack_params>(to_whom, 0, "", epoch, txn_type, nullptr);
            switch (txn_type) {
                case proto::TxnType::RemoteServerTxn : {
                    return SendRemoteServerTxn(epoch, to_whom, txn_ptr, txn_type);
                }
                case proto::TxnType::BackUpTxn : {
                    return EpochMessageSendHandler::SendBackUpTxn(epoch, txn_ptr, txn_type);
                }
                case proto::TxnType::BackUpACK :
                case proto::TxnType::AbortSetACK :
                case proto::TxnType::InsertSetACK :
                case proto::TxnType::EpochShardingACK : {
                    return SendACK(epoch, to_whom, txn_type);
                }
                case proto::TxnType::EpochLogPushDownComplete : {
                    return SendMessageToAll(epoch, txn_type);
                }
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
        }
        return true;
    }

    bool EpochMessageSendHandler::SendRemoteServerTxn(uint64_t& epoch, uint64_t& to_whom, const std::shared_ptr<proto::Transaction>& txn_ptr, proto::TxnType txn_type) {
        auto msg = std::make_unique<proto::Message>();
        auto* txn_temp = msg->mutable_txn();
        *(txn_temp) = *txn_ptr;
        txn_temp->set_txn_type(txn_type);
        auto serialized_txn_str_ptr = std::make_unique<std::string>();
        Gzip(msg.get(), serialized_txn_str_ptr.get());
        assert(!serialized_txn_str_ptr->empty());
        if (ctx.taasContext.taas_mode == TaasMode::MultiMaster) {
            MessageQueue::send_to_server_pub_queue->enqueue(
                    std::make_unique<send_params>(0, 0, "", epoch, txn_type, std::move(serialized_txn_str_ptr),nullptr));
            return MessageQueue::send_to_server_pub_queue->enqueue(
                    std::make_unique<send_params>(0, 0, "", epoch, proto::TxnType::NullMark,nullptr, nullptr));
        } else {
            MessageQueue::send_to_server_queue->enqueue(
                    std::make_unique<send_params>(to_whom, 0, "", epoch, txn_type,std::move(serialized_txn_str_ptr), nullptr));
            return MessageQueue::send_to_server_queue->enqueue(
                    std::make_unique<send_params>(0, 0, "", epoch, proto::TxnType::NullMark,nullptr, nullptr));
        }
    }

    bool EpochMessageSendHandler::SendBackUpTxn(uint64_t& epoch, const std::shared_ptr<proto::Transaction>& txn_ptr, proto::TxnType txn_type) {
        auto msg = std::make_unique<proto::Message>();
        auto* txn_temp = msg->mutable_txn();
        *(txn_temp) = *txn_ptr;
        txn_temp->set_txn_type(txn_type);
        auto serialized_txn_str_ptr = std::make_unique<std::string>();
        Gzip(msg.get(), serialized_txn_str_ptr.get());
        assert(!serialized_txn_str_ptr->empty());
        MessageQueue::send_to_server_pub_queue->enqueue(std::make_unique<send_params>(0, 0, "", epoch, txn_type, std::move(serialized_txn_str_ptr),nullptr));
        return MessageQueue::send_to_server_pub_queue->enqueue(std::make_unique<send_params>(0, 0, "",epoch, proto::TxnType::NullMark,nullptr, nullptr));
    }

    bool EpochMessageSendHandler::SendACK(uint64_t &epoch, uint64_t &to_whom, proto::TxnType txn_type) {
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
        MessageQueue::send_to_server_queue->enqueue(std::make_unique<send_params>(to_whom, 0, "", epoch, txn_type, std::move(serialized_txn_str_ptr),nullptr));
        return MessageQueue::send_to_server_queue->enqueue(std::make_unique<send_params>(0, 0, "", epoch, proto::TxnType::NullMark, nullptr, nullptr));
    }

    bool EpochMessageSendHandler::SendMessageToAll(uint64_t& epoch, proto::TxnType txn_type) {
        auto msg = std::make_unique<proto::Message>();
        auto* txn_end = msg->mutable_txn();
        txn_end->set_server_id(ctx.taasContext.txn_node_ip_index);
        txn_end->set_txn_type(txn_type);
        txn_end->set_commit_epoch(epoch);
        txn_end->set_sharding_id(0);
        auto serialized_txn_str_ptr = std::make_unique<std::string>();
        Gzip(msg.get(), serialized_txn_str_ptr.get());
        MessageQueue::send_to_server_pub_queue->enqueue(
                std::make_unique<send_params>(0, 0, "", epoch, txn_type, std::move(serialized_txn_str_ptr),
                                              nullptr));
        return MessageQueue::send_to_server_pub_queue->enqueue(std::make_unique<send_params>(0, 0, "", epoch, proto::TxnType::NullMark, nullptr, nullptr));
    }


    bool EpochMessageSendHandler::SendEpochEndMessage(const uint64_t &txn_node_ip_index, const uint64_t epoch, const uint64_t &kTxnNodeNum) {
        for(uint64_t server_id = 0; server_id < kTxnNodeNum; server_id ++) {
            if (server_id == txn_node_ip_index) continue;
            auto msg = std::make_unique<proto::Message>();
            auto *txn_end = msg->mutable_txn();
            txn_end->set_server_id(txn_node_ip_index);
            txn_end->set_txn_type(proto::TxnType::EpochEndFlag);
            txn_end->set_commit_epoch(epoch);
            txn_end->set_sharding_id(server_id);
            txn_end->set_csn(EpochMessageReceiveHandler::sharding_should_send_txn_num.GetCount(epoch)); /// 不同server由不同的数量
            auto serialized_txn_str_ptr = std::make_unique<std::string>();
            Gzip(msg.get(), serialized_txn_str_ptr.get());
            MessageQueue::send_to_server_queue->enqueue(std::make_unique<send_params>(server_id, 0, "", epoch,proto::TxnType::EpochEndFlag,std::move(serialized_txn_str_ptr),nullptr));
            MessageQueue::send_to_server_queue->enqueue(std::make_unique<send_params>(0, 0, "", epoch, proto::TxnType::NullMark,nullptr, nullptr));
        }
        auto msg = std::make_unique<proto::Message>();
        auto* txn_end = msg->mutable_txn();
        txn_end->set_server_id(txn_node_ip_index);
        txn_end->set_txn_type(proto::TxnType::BackUpEpochEndFlag);
        txn_end->set_commit_epoch(epoch);
        txn_end->set_sharding_id(0);
        txn_end->set_csn(static_cast<uint64_t>(EpochMessageReceiveHandler::backup_should_send_txn_num.GetCount(backup_sent_epoch)));
        auto serialized_txn_str_ptr = std::make_unique<std::string>();
        Gzip(msg.get(), serialized_txn_str_ptr.get());
        MessageQueue::send_to_server_pub_queue->enqueue(std::make_unique<send_params>(0, 0, "", epoch, proto::TxnType::BackUpEpochEndFlag, std::move(serialized_txn_str_ptr), nullptr));
        return MessageQueue::send_to_server_pub_queue->enqueue(std::make_unique<send_params>(0, 0, "", epoch, proto::TxnType::NullMark, nullptr, nullptr));
    }

    bool EpochMessageSendHandler::SendAbortSet(const uint64_t &txn_node_ip_index, const uint64_t epoch, const uint64_t &kCacheMaxLength) {
        auto msg = std::make_unique<proto::Message>();
        auto *txn_end = msg->mutable_txn();
        txn_end->set_server_id(txn_node_ip_index);
        txn_end->set_txn_type(proto::TxnType::AbortSet);
        txn_end->set_commit_epoch(epoch);
        txn_end->set_sharding_id(0);
        std::vector<std::string> keys, values;
        Merger::local_epoch_abort_txn_set[epoch % kCacheMaxLength]->getValue(keys, values);
        for (uint64_t i = 0; i < keys.size(); i++) {
            auto row = txn_end->add_row();
            row->set_key(keys[i]);
            row->set_data(values[i]);
        }
        auto serialized_txn_str_ptr = std::make_unique<std::string>();
        Gzip(msg.get(), serialized_txn_str_ptr.get());
        MessageQueue::send_to_server_pub_queue->enqueue(std::make_unique<send_params>(0, 0, "", epoch, proto::TxnType::AbortSet, std::move(serialized_txn_str_ptr), nullptr));
        return MessageQueue::send_to_server_pub_queue->enqueue( std::make_unique<send_params>(0, 0, "", epoch, proto::TxnType::NullMark, nullptr, nullptr));
    }

}