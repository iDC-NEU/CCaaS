//
// Created by 周慰星 on 11/9/22.
//
#include <queue>
#include "message/handler.h"
#include "epoch/epoch_manager.h"
#include "utils/utilities.h"

namespace Taas {

    AtomicCounters_Cache ///epoch, index, value
        MessageHandler::sharding_should_receive_pack_num(10, 2), MessageHandler::sharding_received_pack_num(10, 2),
        MessageHandler::sharding_should_receive_txn_num(10, 2), MessageHandler::sharding_received_txn_num(10, 2);
    AtomicCounters_Cache ///epoch, index, value
        MessageHandler::sharding_should_handle_local_txn_num(10, 2), MessageHandler::sharding_handled_local_txn_num(10, 2),
        MessageHandler::sharding_should_send_pack_num(10, 2), MessageHandler::sharding_send_pack_num(10, 2),
        MessageHandler::sharding_should_send_txn_num(10, 2), MessageHandler::sharding_send_txn_num(10, 2);

    AtomicCounters_Cache ///epoch, index, value
        MessageHandler::backup_should_receive_pack_num(10, 2), MessageHandler::backup_received_pack_num(10, 2),
        MessageHandler::backup_should_receive_txn_num(10, 2), MessageHandler::backup_received_txn_num(10, 2);

    bool MessageHandler::Sharding() {
        std::vector<std::unique_ptr<proto::Transaction>> sharding_row_vector;
        for(uint64_t i = 0; i < sharding_num; i ++) {
            sharding_row_vector.emplace_back(std::make_unique<proto::Transaction>());
            sharding_row_vector[i]->set_csn(txn_ptr->csn());
            sharding_row_vector[i]->set_commit_epoch(txn_ptr->commit_epoch());

            sharding_row_vector[i]->set_server_id(txn_ptr->server_id());

            sharding_row_vector[i]->set_client_ip(txn_ptr->client_ip());
            sharding_row_vector[i]->set_client_txn_id(txn_ptr->client_txn_id());

            sharding_row_vector[i]->set_sharding_id(i);
        }
        for(auto i = 0; i < txn_ptr->row_size(); i ++) {
            const auto& row = txn_ptr->row(i);
            if(row.op_type() == proto::OpType::Read) {
                continue;
            }
            else {
                auto row_ptr = sharding_row_vector[GetHashValue(row.key())]->add_row();
                (*row_ptr) = row;
            }
        }
        for(uint64_t i = 0; i < sharding_num; i ++) {
            if(sharding_row_vector[i]->row_size() > 0) {
                sharding_should_send_txn_num.IncCount(txn_ptr->commit_epoch(), i, 1);
                ///sharding sending
                if(i == ctx.txn_node_ip_index) {
                    sharding_cache[txn_ptr->commit_epoch() % ctx.kCacheMaxLength][ctx.txn_node_ip_index]->push(std::move(txn_ptr));
                }
                else {

                }
                {///backup sending

                }
            }
            else {
                /// do nothing;
            }
        }
    }

    bool MessageHandler::HandleReceivedClientTxn() {
        txn_ptr->set_commit_epoch(EpochManager::GetPhysicalEpoch());
        EpochManager::local_should_exec_txn_num.IncCount(txn_ptr->commit_epoch(),
                                                         thread_id, 1);
        txn_ptr->set_csn(now_to_us());
        txn_ptr->set_server_id(ctx.txn_node_ip_index);
        Sharding();

    }

    bool MessageHandler::HandleReceivedServerTxn() {
        auto message_epoch_mod = txn_ptr->commit_epoch() % ctx.kCacheMaxLength;
        auto message_server_id = txn_ptr->server_id();
        auto sharding_id = txn_ptr->sharding_id();
        ///这里需要注意 这几个计数器是以server_id为粒度增加的，不是线程id ！！！
        if ((EpochManager::GetLogicalEpoch() % ctx.kCacheMaxLength) ==
            ((message_epoch_mod + 2) % ctx.kCacheMaxLength))
            assert(false);

        if (txn_ptr->txn_type() == proto::TxnType::RemoteServerTxn) {
            sharding_cache[message_epoch_mod][message_server_id]->push(std::move(txn_ptr));
            sharding_received_txn_num.IncCount(message_epoch_mod,message_server_id, 1);
            EpochManager::remote_received_txn_num.IncCount(message_epoch_mod,message_server_id, 1);
        }
        else if (txn_ptr->txn_type() == proto::TxnType::EpochEndFlag) {
            sharding_should_receive_txn_num.IncCount(message_epoch_mod,message_server_id,txn_ptr->csn());
            sharding_received_pack_num.IncCount(message_epoch_mod,message_server_id, 1);
            EpochManager::remote_should_receive_txn_num.IncCount(message_epoch_mod,message_server_id,txn_ptr->csn());
            EpochManager::remote_received_pack_num.IncCount(message_epoch_mod,message_server_id, 1);
        }
        else if (txn_ptr->txn_type() == proto::TxnType::BackUpTxn) {
            backup_cache[message_epoch_mod][sharding_id]->push(std::move(txn_ptr));
            backup_received_txn_num.IncCount(message_epoch_mod,sharding_id, 1);
        }
        else if (txn_ptr->txn_type() == proto::TxnType::BackUpEpochEndFlag) {
            backup_should_receive_txn_num.IncCount(message_epoch_mod,sharding_id,txn_ptr->csn());
            backup_should_receive_pack_num.IncCount(message_epoch_mod,sharding_id, 1);
        }
        else if (txn_ptr->txn_type() == proto::TxnType::AbortSet) {
            ///abort set;
        }
        else if (txn_ptr->txn_type() == proto::TxnType::InsertSet) {
            ///insert set;
        }

    }


    bool MessageHandler::HandleReceivedMessage() {
        sleep_flag = false;
        while (listen_message_queue.try_dequeue(message_ptr)) {
            if (message_ptr->empty()) continue;
            message_string_ptr = std::make_unique<std::string>(static_cast<const char *>(message_ptr->data()),
                                                                    message_ptr->size());
            msg_ptr = std::make_unique<proto::Message>();
            res = UnGzip(msg_ptr.get(), message_string_ptr.get());
            assert(res);
            if (msg_ptr->type_case() == proto::Message::TypeCase::kTxn) {
                txn_ptr = std::make_unique<proto::Transaction>(*(msg_ptr->release_txn()));
                if (txn_ptr->txn_type() == proto::TxnType::ClientTxn) {/// sql node --> txn node
                    HandleReceivedClientTxn();
                } else {/// txn node -> txn node
                    HandleReceivedServerTxn();
                }
            } else {
                request_queue.enqueue(std::move(msg_ptr));
                request_queue.enqueue(nullptr);
            }
            sleep_flag = true;
        }
        return sleep_flag;
    }

//    bool MessageHandler::HandleLocalMergedTxn() {
//        sleep_flag = false;
//        while(first_merged_queue.try_dequeue(txn_ptr)) {/// 缓存本地第一次merge后的事务
//            if(txn_ptr == nullptr) continue;
//            sharding_cache[txn_ptr->commit_epoch() % ctx.kCacheMaxLength][ctx.txn_node_ip_index]->push(std::move(txn_ptr));
//            sleep_flag = true;
//        }
//        return sleep_flag;
//    }

    bool MessageHandler::CheckTxnReceiveComplete() const {
        return
                EpochManager::server_state.GetCount(server_dequeue_id) <=
                EpochManager::remote_received_pack_num.GetCount(epoch_mod, server_dequeue_id) &&

                EpochManager::remote_received_txn_num.GetCount(epoch_mod, server_dequeue_id) >=
                EpochManager::remote_should_receive_txn_num.GetCount(epoch_mod, server_dequeue_id);
    }


/**
 * @brief check cache
 *
 * @param ctx XML中的配置相关信息
 * @return true
 * @return false
 */
    bool MessageHandler::HandleTxnCache() {
        epoch_mod = EpochManager::GetLogicalEpoch() % ctx.kCacheMaxLength;
        sleep_flag = false;
        if (epoch != epoch_mod) {
            clear_epoch = epoch;
            epoch = epoch_mod;
        }
        ///收集一个epoch的完整的写集后才能放到merge_queue中
        if (server_dequeue_id != ctx.txn_node_ip_index &&
            MessageHandler::CheckTxnReceiveComplete()) {
            while (!sharding_cache[epoch_mod][server_dequeue_id]->empty()) {
                auto txn_ptr_tmp = std::move(sharding_cache[epoch_mod][server_dequeue_id]->front());
                sharding_cache[epoch_mod][server_dequeue_id]->pop();
                if (!merge_queue.enqueue(std::move(txn_ptr_tmp))) {
                    assert(false);
                }
                if (!merge_queue.enqueue(nullptr)) {
                    assert(false);
                }
                EpochManager::should_merge_txn_num.IncCount(epoch_mod, thread_id, 1);
                EpochManager::enqueued_txn_num.IncCount(epoch_mod, server_dequeue_id, 1);
            }
            while (!sharding_cache[clear_epoch][server_dequeue_id]->empty())
                sharding_cache[clear_epoch][server_dequeue_id]->pop();
            sleep_flag = true;
        }
        server_dequeue_id = (server_dequeue_id + 1) % ctx.kTxnNodeNum;
        ///local txn -> merge_queue
        while (!sharding_cache[epoch_mod][ctx.txn_node_ip_index]->empty()) {
            auto txn_ptr_tmp = std::move(sharding_cache[epoch_mod][ctx.txn_node_ip_index]->front());
            sharding_cache[epoch_mod][ctx.txn_node_ip_index]->pop();
            if (!merge_queue.enqueue(std::move(txn_ptr_tmp))) {
                assert(false);
            }
            if (!merge_queue.enqueue(nullptr)) {
                assert(false);
            }
            EpochManager::enqueued_txn_num.IncCount(epoch_mod, ctx.txn_node_ip_index, 1);
            sleep_flag = true;
        }
        return sleep_flag;
    }

    bool MessageHandler::Init(uint64_t id, Context context) {
        message_ptr = nullptr;
        txn_ptr = nullptr;
        pack_param = nullptr;
        server_dequeue_id = 0, epoch_mod = 0, epoch = 0, clear_epoch = 0, max_length = 0;
        res = false, sleep_flag = false;

        thread_id = id;
        ctx = std::move(context);
        max_length = ctx.kCacheMaxLength;
        sharding_num = ctx.kTxnNodeNum;

        MessageHandler::sharding_should_receive_pack_num.Init(max_length, ctx.kIndexNum),
        MessageHandler::sharding_received_pack_num.Init(max_length, ctx.kIndexNum),
        MessageHandler::sharding_should_receive_txn_num.Init(max_length, ctx.kIndexNum),
        MessageHandler::sharding_received_txn_num.Init(max_length, ctx.kIndexNum),
        MessageHandler::sharding_should_handle_local_txn_num.Init(max_length, ctx.kIndexNum),
        MessageHandler::sharding_handled_local_txn_num.Init(max_length, ctx.kIndexNum),
        MessageHandler::sharding_should_send_pack_num.Init(max_length, ctx.kIndexNum),
        MessageHandler::sharding_send_pack_num.Init(max_length, ctx.kIndexNum),
        MessageHandler::sharding_should_send_txn_num.Init(max_length, ctx.kIndexNum),
        MessageHandler::sharding_send_txn_num.Init(max_length, ctx.kIndexNum),

        MessageHandler::backup_should_receive_pack_num.Init(max_length, ctx.kIndexNum),
        MessageHandler::backup_received_pack_num.Init(max_length, ctx.kIndexNum),
        MessageHandler::backup_should_receive_txn_num.Init(max_length, ctx.kIndexNum),
        MessageHandler::backup_received_txn_num.Init(max_length, ctx.kIndexNum),

        sharding_cache.reserve( + 1);
        backup_cache.reserve( + 1);
        for(int i = 0; i < (int)max_length; i ++) {
            sharding_cache.emplace_back(std::vector<std::unique_ptr<std::queue<std::unique_ptr<proto::Transaction>>>>());
            backup_cache.emplace_back(std::vector<std::unique_ptr<std::queue<std::unique_ptr<proto::Transaction>>>>());
            for(int j = 0; j <= (int)ctx.kTxnNodeNum; j++){
                sharding_cache[i].push_back(std::make_unique<std::queue<std::unique_ptr<proto::Transaction>>>());
                backup_cache[i].push_back(std::make_unique<std::queue<std::unique_ptr<proto::Transaction>>>());
            }
        }
        return true;
    }


}