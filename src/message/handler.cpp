//
// Created by 周慰星 on 11/9/22.
//
#include <queue>
#include "message/handler.h"
#include "epoch/epoch_manager.h"
#include "utils/utilities.h"

namespace Taas {
    ///监听sql node写集 或使用同一个线程监听，在后面handler中进行分类处理。

    bool MessageHandler::HandleReceiveMessage() {
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
                if (txn_ptr->txn_type() == proto::TxnType::ClientTxn) { /// sql node --> txn node
                    ///local 来自sql node的事务，local_txn_queue 进行第一次预处理。 处理完成后会放入first_merged_queue，再有cache thread进行缓存

                    txn_ptr->set_commit_epoch(EpochManager::GetPhysicalEpoch());
                    EpochManager::local_should_exec_txn_num.IncCount(txn_ptr->commit_epoch(),
                                                                     thread_id, 1);
                    txn_ptr->set_csn(now_to_us());
                    txn_ptr->set_server_id(ctx.txn_node_ip_index);
//                    printf("txn time: %llu, id: %llu \n", txn_ptr->csn(), txn_ptr->client_txn_id());
                    local_txn_queue.enqueue(std::move(txn_ptr));
                } else {/// txn node -> txn node
                    auto message_epoch_mod = txn_ptr->commit_epoch() % ctx.kCacheMaxLength;
                    auto message_server_id = txn_ptr->server_id();
                    ///这里需要注意 这几个计数器是以server_id为粒度增加的，不是线程id ！！！
                    if (txn_ptr->txn_type() == proto::TxnType::EpochEndFlag) {
                        if ((EpochManager::GetLogicalEpoch() % ctx.kCacheMaxLength) ==
                            ((message_epoch_mod + 2) % ctx.kCacheMaxLength))
                            assert(false);
                        EpochManager::remote_should_receive_txn_num.IncCount(message_epoch_mod,
                                                                            message_server_id,txn_ptr->csn());
                        EpochManager::remote_received_pack_num.IncCount(message_epoch_mod,
                                                                        message_server_id, 1);
//                        printf("收到来自远端服务器的事务 server_id %llu, epoch_num %llu, txn_num %llu\n",
//                               message_server_id, txn_ptr->commit_epoch(), txn_ptr->csn());
                    } else {
                        message_cache[message_epoch_mod][message_server_id]->push(std::move(txn_ptr));
                        EpochManager::remote_received_txn_num.IncCount(message_epoch_mod,
                                                                       message_server_id, 1);
                    }
                }
            } else {
                request_queue.enqueue(std::move(msg_ptr));
                request_queue.enqueue(nullptr);
            }
            sleep_flag = true;
        }
        return sleep_flag;
    }

    bool MessageHandler::HandleLocalMergedTxn() {
        sleep_flag = false;
        while(first_merged_queue.try_dequeue(txn_ptr)) {/// 缓存本地第一次merge后的事务
            if(txn_ptr == nullptr) continue;
            message_cache[txn_ptr->commit_epoch() % ctx.kCacheMaxLength][ctx.txn_node_ip_index]->push(std::move(txn_ptr));
            sleep_flag = true;
        }
        return sleep_flag;
    }

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
    bool MessageHandler::HandleTxnCachea() {
        epoch_mod = EpochManager::GetLogicalEpoch() % ctx.kCacheMaxLength;
        sleep_flag = false;
        if (epoch != epoch_mod) {
            clear_epoch = epoch;
            epoch = epoch_mod;
        }
        ///收集一个epoch的完整的写集后才能放到merge_queue中
        if (server_dequeue_id != ctx.txn_node_ip_index &&
            MessageHandler::CheckTxnReceiveComplete()) {
            while (!message_cache[epoch_mod][server_dequeue_id]->empty()) {
                auto txn_ptr_tmp = std::move(message_cache[epoch_mod][server_dequeue_id]->front());
                message_cache[epoch_mod][server_dequeue_id]->pop();
                if (!merge_queue.enqueue(std::move(txn_ptr_tmp))) {
                    assert(false);
                }
                if (!merge_queue.enqueue(nullptr)) {
                    assert(false);
                }
                EpochManager::should_merge_txn_num.IncCount(epoch_mod, thread_id, 1);
                EpochManager::enqueued_txn_num.IncCount(epoch_mod, server_dequeue_id, 1);
            }
            while (!message_cache[clear_epoch][server_dequeue_id]->empty())
                message_cache[clear_epoch][server_dequeue_id]->pop();
            sleep_flag = true;
        }
        server_dequeue_id = (server_dequeue_id + 1) % ctx.kTxnNodeNum;
        ///local txn -> merge_queue
        while (!message_cache[epoch_mod][ctx.txn_node_ip_index]->empty()) {
            auto txn_ptr_tmp = std::move(message_cache[epoch_mod][ctx.txn_node_ip_index]->front());
            message_cache[epoch_mod][ctx.txn_node_ip_index]->pop();
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

        message_cache.reserve( + 1);
        for(int i = 0; i < (int)max_length; i ++) {
            message_cache.emplace_back(std::vector<std::unique_ptr<std::queue<std::unique_ptr<proto::Transaction>>>>());
            for(int j = 0; j <= (int)ctx.kTxnNodeNum; j++){
                message_cache[i].push_back(std::make_unique<std::queue<std::unique_ptr<proto::Transaction>>>());
            }
        }
        return true;
    }


}