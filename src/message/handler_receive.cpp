//
// Created by 周慰星 on 11/9/22.
//
#include <queue>
#include <utility>
#include "message/handler_receive.h"
#include "epoch/epoch_manager.h"
#include "message/handler_send.h"
#include "tools/utilities.h"

namespace Taas {

    AtomicCounters_Cache ///epoch, index, value
        MessageReceiveHandler::sharding_should_handle_local_txn_num(10, 2),
        MessageReceiveHandler::sharding_handled_local_txn_num(10, 2),
        MessageReceiveHandler::sharding_should_send_txn_num(10, 2),
        MessageReceiveHandler::sharding_send_txn_num(10, 2);

    AtomicCounters_Cache ///epoch, index, value
        MessageReceiveHandler::sharding_should_receive_pack_num(10, 2),
        MessageReceiveHandler::sharding_received_pack_num(10, 2),
        MessageReceiveHandler::sharding_should_receive_txn_num(10, 2),
        MessageReceiveHandler::sharding_received_txn_num(10, 2),
        MessageReceiveHandler::sharding_should_enqueue_merge_queue_txn_num(10, 2),
        MessageReceiveHandler::sharding_enqueued_merge_queue_txn_num(10, 2),
        MessageReceiveHandler::should_enqueue_local_txn_queue_txn_num(10, 2),
        MessageReceiveHandler::enqueued_local_txn_queue_txn_num(10, 2);

    AtomicCounters_Cache ///epoch, index, value
        MessageReceiveHandler::backup_should_send_txn_num(10, 2),
        MessageReceiveHandler::backup_send_txn_num(10, 2),
        MessageReceiveHandler::backup_should_receive_pack_num(10, 2),
        MessageReceiveHandler::backup_received_pack_num(10, 2),
        MessageReceiveHandler::backup_should_receive_txn_num(10, 2),
        MessageReceiveHandler::backup_received_txn_num(10, 2),
        MessageReceiveHandler::backup_received_ack_num(10, 2);

    AtomicCounters_Cache ///epoch, index, value
        MessageReceiveHandler::insert_set_should_receive_num(10, 2),
        MessageReceiveHandler::insert_set_received_num(10, 2),
        MessageReceiveHandler::insert_set_received_ack_num(10, 2);

    AtomicCounters_Cache ///epoch, index, value
        MessageReceiveHandler::sharding_should_receive_abort_set_num(10, 2),
        MessageReceiveHandler::sharding_received_abort_set_num(10, 2),
        MessageReceiveHandler::sharding_abort_set_received_ack_num(10, 2);



    bool MessageReceiveHandler::Sharding() {
        sharding_should_handle_local_txn_num.IncCount(message_epoch, thread_id, 1);
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
            auto row_ptr = sharding_row_vector[GetHashValue(row.key())]->add_row();
            (*row_ptr) = row;
        }
        for(uint64_t i = 0; i < sharding_num; i ++) {
            if(sharding_row_vector[i]->row_size() > 0) {
                ///sharding sending
                if(i == ctx.txn_node_ip_index) {
                    sharding_should_enqueue_merge_queue_txn_num.IncCount(message_epoch, i, 1);
                    sharding_cache[message_epoch_mod][ctx.txn_node_ip_index]->push(std::move(sharding_row_vector[i]));
                }
                else {
                    sharding_should_send_txn_num.IncCount(message_epoch, i, 1);
                    MessageSendHandler::SendTxnToPackThread(ctx, *(sharding_row_vector[i]), proto::TxnType::RemoteServerTxn);
                }
            }
        }
        {///backup sending full txn
            backup_should_send_txn_num.IncCount(message_epoch, ctx.txn_node_ip_index, 1);
            MessageSendHandler::SendTxnToPackThread(ctx, *(txn_ptr), proto::TxnType::BackUpTxn);
        }
        sharding_handled_local_txn_num.IncCount(message_epoch, thread_id, 1);
        return true;
    }

    bool MessageReceiveHandler::UpdateEpochAbortSet() {
        message_epoch = txn_ptr->commit_epoch();
        message_epoch_mod = txn_ptr->commit_epoch() % ctx.kCacheMaxLength;
        for(int i = 0; i < txn_ptr->row_size(); i ++) {
            EpochManager::epoch_insert_set[message_epoch_mod]->insert(txn_ptr->row(i).key(), txn_ptr->row(i).data());
        }
    }

    bool MessageReceiveHandler::HandleReceivedTxn() {
        message_epoch = txn_ptr->commit_epoch();
        message_epoch_mod = txn_ptr->commit_epoch() % ctx.kCacheMaxLength;
        message_server_id = txn_ptr->server_id();
        message_sharding_id = txn_ptr->sharding_id();
        ///这里需要注意 这几个计数器是以server_id为粒度增加的，不是线程id ！！！

        switch (txn_ptr->txn_type()) {
            case proto::TxnType::ClientTxn : {/// sql node --> txn node
                txn_ptr->set_commit_epoch(EpochManager::GetPhysicalEpoch());
                txn_ptr->set_csn(now_to_us());
                txn_ptr->set_server_id(ctx.txn_node_ip_index);
                message_epoch = txn_ptr->commit_epoch();
                message_epoch_mod = txn_ptr->commit_epoch() % ctx.kCacheMaxLength;
                message_server_id = txn_ptr->server_id();
                message_sharding_id = txn_ptr->sharding_id();
                Sharding();
                assert(txn_ptr != nullptr);
                local_txn_cache[message_epoch_mod][message_server_id]->push(std::move(txn_ptr));
                break;
            }
            case proto::TxnType::RemoteServerTxn : {
                printf("receive remote txn\n");
                if ((EpochManager::GetLogicalEpoch() % ctx.kCacheMaxLength) ==
                    ((message_epoch_mod + 55) % ctx.kCacheMaxLength))
                    assert(false);
                sharding_received_txn_num.IncCount(message_epoch,message_sharding_id, 1);
                sharding_cache[message_epoch_mod][message_sharding_id]->push(std::move(txn_ptr));
                break;
            }
            case proto::TxnType::EpochEndFlag : {
                printf("receive remote end flag\n");
                sharding_should_receive_txn_num.IncCount(message_epoch,message_sharding_id,txn_ptr->csn());
                sharding_received_pack_num.IncCount(message_epoch,message_sharding_id, 1);
                break;
            }
            case proto::CommittedTxn:
                break;
            case proto::TxnType::BackUpTxn : {
                printf("receive remote backup txn\n");
                if ((EpochManager::GetLogicalEpoch() % ctx.kCacheMaxLength) ==
                    ((message_epoch_mod + 55) % ctx.kCacheMaxLength))
                    assert(false);
                backup_received_txn_num.IncCount(message_epoch,message_sharding_id, 1);
                backup_cache[message_epoch_mod][message_sharding_id]->push(std::move(txn_ptr));
                break;
            }
            case proto::TxnType::BackUpEpochEndFlag : {
                printf("receive remote backup end flag\n");
                backup_should_receive_txn_num.IncCount(message_epoch,message_sharding_id,txn_ptr->csn());
                backup_received_pack_num.IncCount(message_epoch,message_sharding_id, 1);
                break;
            }
            case proto::TxnType::AbortSet : {
                printf("receive abort\n");
                UpdateEpochAbortSet();
                sharding_received_abort_set_num.IncCount(message_epoch,message_sharding_id, 1);
                abort_set_cache[message_epoch_mod][message_sharding_id]->push(std::move(txn_ptr));
                break;
            }
            case proto::TxnType::InsertSet : {
                printf("receive insert \n");
                insert_set_received_num.IncCount(message_epoch,message_sharding_id, 1);
                insert_set_cache[message_epoch_mod][message_sharding_id]->push(std::move(txn_ptr));
                break;
            }
            case proto::TxnType::BackUpACK : {
                printf("receive backup ack\n");
                backup_received_ack_num.IncCount(message_epoch,message_server_id, 1);
                break;
            }
            case proto::TxnType::AbortSetACK : {
                printf("receive abort ack\n");
                sharding_abort_set_received_ack_num.IncCount(message_epoch,message_server_id, 1);
                break;
            }
            case proto::TxnType::InsertSetACK : {
                printf("receive insert ack\n");
                insert_set_received_ack_num.IncCount(message_epoch,message_server_id, 1);
                break;
            }
            case proto::NullMark:
                break;
            case proto::TxnType_INT_MIN_SENTINEL_DO_NOT_USE_:
                break;
            case proto::TxnType_INT_MAX_SENTINEL_DO_NOT_USE_:
                break;
        }


        return true;
    }


    bool MessageReceiveHandler::HandleReceivedMessage() {
        sleep_flag = false;
        while (listen_message_queue.try_dequeue(message_ptr)) {
//            printf("handler receive a message\n");
            if (message_ptr->empty()) continue;
            message_string_ptr = std::make_unique<std::string>(static_cast<const char *>(message_ptr->data()),
                                                                    message_ptr->size());
            msg_ptr = std::make_unique<proto::Message>();
            res = UnGzip(msg_ptr.get(), message_string_ptr.get());
            assert(res);
            if (msg_ptr->type_case() == proto::Message::TypeCase::kTxn) {
                txn_ptr = std::make_unique<proto::Transaction>(*(msg_ptr->release_txn()));
//                printf("handle message:txn\n");
                HandleReceivedTxn();
            } else {
                request_queue.enqueue(std::move(msg_ptr));
                request_queue.enqueue(nullptr);
            }
            sleep_flag = true;
        }
        return sleep_flag;
    }


    bool MessageReceiveHandler::CheckTxnReceiveComplete() const {
        return
                EpochManager::server_state.GetCount(server_dequeue_id) <=
                sharding_received_pack_num.GetCount(epoch_mod, server_dequeue_id) &&

                sharding_should_receive_txn_num.GetCount(epoch_mod, server_dequeue_id) >=
                sharding_received_txn_num.GetCount(epoch_mod, server_dequeue_id);
    }


/**
 * @brief check cache
 *
 * @param ctx XML中的配置相关信息
 * @return true
 * @return false
 */
    bool MessageReceiveHandler::HandleTxnCache() {
        epoch_mod = EpochManager::GetLogicalEpoch() % ctx.kCacheMaxLength;
        sleep_flag = false;
        if (epoch != epoch_mod) {
            clear_epoch = epoch;
            epoch = epoch_mod;
        }

        ///收集一个epoch的完整的写集后才能放到merge_queue中
        ///将远端发送过来的当前epoch的（分片）事务进行合并，生成abort set
        if (server_dequeue_id != ctx.txn_node_ip_index &&
            MessageReceiveHandler::CheckTxnReceiveComplete()) {
            while (!sharding_cache[epoch_mod][server_dequeue_id]->empty()) {
                auto txn_ptr_tmp = std::move(sharding_cache[epoch_mod][server_dequeue_id]->front());
                sharding_cache[epoch_mod][server_dequeue_id]->pop();
                sharding_should_enqueue_merge_queue_txn_num.IncCount(epoch_mod, server_dequeue_id, 1);
                if (!merge_queue.enqueue(std::move(txn_ptr_tmp))) {
                    assert(false);
                }
                if (!merge_queue.enqueue(nullptr)) {
                    assert(false);
                }
                EpochManager::should_merge_txn_num.IncCount(epoch_mod, server_dequeue_id, 1);
                sharding_enqueued_merge_queue_txn_num.IncCount(epoch_mod, server_dequeue_id, 1);
            }
            while (!sharding_cache[clear_epoch][server_dequeue_id]->empty())
                sharding_cache[clear_epoch][server_dequeue_id]->pop();
            sleep_flag = true;
        }
        server_dequeue_id = (server_dequeue_id + 1) % ctx.kTxnNodeNum;

        ///local txn -> merge_queue 将本地的当前epoch的分片事务进行合并，生成abort set
        while (!sharding_cache[epoch_mod][ctx.txn_node_ip_index]->empty()) {
            auto txn_ptr_tmp = std::move(sharding_cache[epoch_mod][ctx.txn_node_ip_index]->front());
            sharding_cache[epoch_mod][ctx.txn_node_ip_index]->pop();
            if (!merge_queue.enqueue(std::move(txn_ptr_tmp))) {
                assert(false);
            }
            if (!merge_queue.enqueue(nullptr)) {
                assert(false);
            }
            EpochManager::should_merge_txn_num.IncCount(epoch_mod, server_dequeue_id, 1);
            sharding_enqueued_merge_queue_txn_num.IncCount(epoch_mod, ctx.txn_node_ip_index, 1);
            sleep_flag = true;
        }

        ///local txn -> local_txn_queue 做redo log时，使用整个事务，而不是当前分片的自事务
        ///如果使用分片事务做redo log，则使用commit_queue
        while (!local_txn_cache[epoch_mod][ctx.txn_node_ip_index]->empty()) {
            assert(local_txn_cache[epoch_mod][ctx.txn_node_ip_index]->front() != NULL);
            auto txn_ptr_tmp = std::move(local_txn_cache[epoch_mod][ctx.txn_node_ip_index]->front());
            local_txn_cache[epoch_mod][ctx.txn_node_ip_index]->pop();
            assert(txn_ptr_tmp != nullptr);
            if (!local_txn_queue.enqueue(std::move(txn_ptr_tmp))) {
                assert(false);
            }
            if (!local_txn_queue.enqueue(nullptr)) {
                assert(false);
            }
            EpochManager::should_commit_txn_num.IncCount(epoch_mod, server_dequeue_id, 1);
            enqueued_local_txn_queue_txn_num.IncCount(epoch_mod, ctx.txn_node_ip_index, 1);
            sleep_flag = true;
        }

        sleep_flag = sleep_flag | ClearCache();

        return sleep_flag;
    }


    bool MessageReceiveHandler::CheckReceivedStatesAndReply() { /// 该函数只由handler的第一个线程调用，防止多次发送
        res = false;
        if(server_reply_ack_id != ctx.txn_node_ip_index) {
            if(backup_should_receive_pack_num.GetCount(backup_send_ack_epoch_num[server_reply_ack_id], server_reply_ack_id) > 0 &&
                    backup_received_txn_num.GetCount(backup_send_ack_epoch_num[server_reply_ack_id], server_reply_ack_id) ==
                    backup_should_receive_txn_num.GetCount(backup_send_ack_epoch_num[server_reply_ack_id], server_reply_ack_id)
            ) {
                ///send reply message
                MessageSendHandler::SendTaskToPackThread(ctx, backup_send_ack_epoch_num[server_reply_ack_id],
                                                        server_reply_ack_id, proto::TxnType::BackUpACK);
                backup_send_ack_epoch_num[server_reply_ack_id] ++;
                res = true;
            }
            if(sharding_received_abort_set_num.GetCount(backup_insert_set_send_ack_epoch_num[server_reply_ack_id], server_reply_ack_id) > 0) {
                MessageSendHandler::SendTaskToPackThread(ctx, backup_insert_set_send_ack_epoch_num[server_reply_ack_id],
                                                        server_reply_ack_id, proto::TxnType::BackUpACK);
                backup_insert_set_send_ack_epoch_num[server_reply_ack_id] ++;
                res = true;
            }
            if(insert_set_received_num.GetCount(abort_set_send_ack_epoch_num[server_reply_ack_id], server_reply_ack_id) > 0) {
                MessageSendHandler::SendTaskToPackThread(ctx, abort_set_send_ack_epoch_num[server_reply_ack_id],
                                                        server_reply_ack_id, proto::TxnType::BackUpACK);
                abort_set_send_ack_epoch_num[server_reply_ack_id] ++;
                res = true;
            }
        }
        server_reply_ack_id ++;
        return res;
    }










    bool MessageReceiveHandler::Init(uint64_t id, Context context) {
        message_ptr = nullptr;
        txn_ptr = nullptr;
        pack_param = nullptr;
        server_dequeue_id = 0, epoch_mod = 0, epoch = 0, clear_epoch = 0, max_length = 0;
        res = false, sleep_flag = false;
        thread_id = id;
        ctx = std::move(context);
        max_length = ctx.kCacheMaxLength;
        sharding_num = ctx.kTxnNodeNum;
        backup_send_ack_epoch_num.reserve(sharding_num + 1);
        backup_insert_set_send_ack_epoch_num.reserve(sharding_num + 1);
        abort_set_send_ack_epoch_num.reserve(sharding_num + 1);
        for(int i = 0; i <= (int) sharding_num; i ++ ) {
            backup_send_ack_epoch_num.emplace_back(0);
            backup_insert_set_send_ack_epoch_num.emplace_back(0);
            abort_set_send_ack_epoch_num.emplace_back(0);
        }
        sharding_cache.reserve(max_length + 1);
        local_txn_cache.reserve(max_length + 1);
        backup_cache.reserve(max_length + 1);
        insert_set_cache.reserve(max_length+ 1);
        abort_set_cache.reserve(max_length + 1);
        for(int i = 0; i < (int)max_length; i ++) {
            sharding_cache.emplace_back(std::vector<std::unique_ptr<std::queue<std::unique_ptr<proto::Transaction>>>>());
            local_txn_cache.emplace_back(std::vector<std::unique_ptr<std::queue<std::unique_ptr<proto::Transaction>>>>());
            backup_cache.emplace_back(std::vector<std::unique_ptr<std::queue<std::unique_ptr<proto::Transaction>>>>());
            insert_set_cache.emplace_back(std::vector<std::unique_ptr<std::queue<std::unique_ptr<proto::Transaction>>>>());
            abort_set_cache.emplace_back(std::vector<std::unique_ptr<std::queue<std::unique_ptr<proto::Transaction>>>>());
            for(int j = 0; j <= (int)ctx.kTxnNodeNum; j++){
                sharding_cache[i].push_back(std::make_unique<std::queue<std::unique_ptr<proto::Transaction>>>());
                local_txn_cache[i].push_back(std::make_unique<std::queue<std::unique_ptr<proto::Transaction>>>());
                backup_cache[i].push_back(std::make_unique<std::queue<std::unique_ptr<proto::Transaction>>>());
                insert_set_cache[i].push_back(std::make_unique<std::queue<std::unique_ptr<proto::Transaction>>>());
                abort_set_cache[i].push_back(std::make_unique<std::queue<std::unique_ptr<proto::Transaction>>>());
            }
        }
        return true;
    }

    bool MessageReceiveHandler::StaticInit(const Context& context) {
        auto max_length = context.kCacheMaxLength;
        auto sharding_num = context.kTxnNodeNum;
        MessageReceiveHandler::sharding_should_receive_pack_num.Init(max_length, sharding_num),
        MessageReceiveHandler::sharding_received_pack_num.Init(max_length, sharding_num),
        MessageReceiveHandler::sharding_should_receive_txn_num.Init(max_length, sharding_num),
        MessageReceiveHandler::sharding_received_txn_num.Init(max_length, sharding_num),

        MessageReceiveHandler::sharding_should_enqueue_merge_queue_txn_num.Init(max_length, sharding_num),
        MessageReceiveHandler::sharding_enqueued_merge_queue_txn_num.Init(max_length, sharding_num),
        MessageReceiveHandler::should_enqueue_local_txn_queue_txn_num.Init(max_length, sharding_num),
        MessageReceiveHandler::enqueued_local_txn_queue_txn_num.Init(max_length, sharding_num),

        MessageReceiveHandler::sharding_should_handle_local_txn_num.Init(max_length, sharding_num),
        MessageReceiveHandler::sharding_handled_local_txn_num.Init(max_length, sharding_num),
        MessageReceiveHandler::sharding_should_send_txn_num.Init(max_length, sharding_num),
        MessageReceiveHandler::sharding_send_txn_num.Init(max_length, sharding_num),

        MessageReceiveHandler::backup_should_send_txn_num.Init(max_length, sharding_num),
        MessageReceiveHandler::backup_send_txn_num.Init(max_length, sharding_num),
        MessageReceiveHandler::backup_should_receive_pack_num.Init(max_length, sharding_num),
        MessageReceiveHandler::backup_received_pack_num.Init(max_length, sharding_num),
        MessageReceiveHandler::backup_should_receive_txn_num.Init(max_length, sharding_num),
        MessageReceiveHandler::backup_received_txn_num.Init(max_length, sharding_num),
        MessageReceiveHandler::backup_received_ack_num.Init(max_length, sharding_num),


        MessageReceiveHandler::insert_set_should_receive_num.Init(max_length, sharding_num),
        MessageReceiveHandler::insert_set_received_num.Init(max_length, sharding_num),
        MessageReceiveHandler::insert_set_received_ack_num.Init(max_length, sharding_num),

        MessageReceiveHandler::sharding_should_receive_abort_set_num.Init(max_length, sharding_num),
        MessageReceiveHandler::sharding_received_abort_set_num.Init(max_length, sharding_num);
        MessageReceiveHandler::sharding_abort_set_received_ack_num.Init(max_length, sharding_num);

        return true;
    }

    bool MessageReceiveHandler::Clear() {
        MessageReceiveHandler::sharding_should_receive_pack_num.Clear(),
        MessageReceiveHandler::sharding_received_pack_num.Clear(),
        MessageReceiveHandler::sharding_should_receive_txn_num.Clear(),
        MessageReceiveHandler::sharding_received_txn_num.Clear(),

        MessageReceiveHandler::sharding_should_enqueue_merge_queue_txn_num.Clear(),
        MessageReceiveHandler::sharding_enqueued_merge_queue_txn_num.Clear(),
        MessageReceiveHandler::should_enqueue_local_txn_queue_txn_num.Clear(),
        MessageReceiveHandler::enqueued_local_txn_queue_txn_num.Clear(),

        MessageReceiveHandler::sharding_should_handle_local_txn_num.Clear(),
        MessageReceiveHandler::sharding_handled_local_txn_num.Clear(),
        MessageReceiveHandler::sharding_should_send_txn_num.Clear(),
        MessageReceiveHandler::sharding_send_txn_num.Clear(),

        MessageReceiveHandler::backup_should_send_txn_num.Clear(),
        MessageReceiveHandler::backup_send_txn_num.Clear(),
        MessageReceiveHandler::backup_should_receive_pack_num.Clear(),
        MessageReceiveHandler::backup_received_pack_num.Clear(),
        MessageReceiveHandler::backup_should_receive_txn_num.Clear(),
        MessageReceiveHandler::backup_received_txn_num.Clear(),
        MessageReceiveHandler::backup_received_ack_num.Clear(),

        MessageReceiveHandler::insert_set_should_receive_num.Clear(),
        MessageReceiveHandler::insert_set_received_num.Clear(),
        MessageReceiveHandler::insert_set_received_ack_num.Clear(),

        MessageReceiveHandler::sharding_should_receive_abort_set_num.Clear(),
        MessageReceiveHandler::sharding_received_abort_set_num.Clear(),
        MessageReceiveHandler::sharding_abort_set_received_ack_num.Clear();
        return true;
    }

    bool MessageReceiveHandler::Clear(uint64_t epoch) {
        MessageReceiveHandler::sharding_should_receive_pack_num.Clear(epoch, 0),
        MessageReceiveHandler::sharding_received_pack_num.Clear(epoch, 0),
        MessageReceiveHandler::sharding_should_receive_txn_num.Clear(epoch, 0),
        MessageReceiveHandler::sharding_received_txn_num.Clear(epoch, 0),

        MessageReceiveHandler::sharding_should_enqueue_merge_queue_txn_num.Clear(epoch, 0),
        MessageReceiveHandler::sharding_enqueued_merge_queue_txn_num.Clear(epoch, 0),
        MessageReceiveHandler::should_enqueue_local_txn_queue_txn_num.Clear(epoch, 0),
        MessageReceiveHandler::enqueued_local_txn_queue_txn_num.Clear(epoch, 0),

        MessageReceiveHandler::sharding_should_handle_local_txn_num.Clear(epoch, 0),
        MessageReceiveHandler::sharding_handled_local_txn_num.Clear(epoch, 0),
        MessageReceiveHandler::sharding_should_send_txn_num.Clear(epoch, 0),
        MessageReceiveHandler::sharding_send_txn_num.Clear(epoch, 0),

        MessageReceiveHandler::backup_should_send_txn_num.Clear(epoch, 0),
        MessageReceiveHandler::backup_send_txn_num.Clear(epoch, 0),
        MessageReceiveHandler::backup_should_receive_pack_num.Clear(epoch, 0),
        MessageReceiveHandler::backup_received_pack_num.Clear(epoch, 0),
        MessageReceiveHandler::backup_should_receive_txn_num.Clear(epoch, 0),
        MessageReceiveHandler::backup_received_txn_num.Clear(epoch, 0),
        MessageReceiveHandler::backup_received_ack_num.Clear(epoch, 0),

        MessageReceiveHandler::insert_set_should_receive_num.Clear(epoch, 0),
        MessageReceiveHandler::insert_set_received_num.Clear(epoch, 0),
        MessageReceiveHandler::insert_set_received_ack_num.Clear(epoch, 0),

        MessageReceiveHandler::sharding_should_receive_abort_set_num.Clear(epoch, 0),
        MessageReceiveHandler::sharding_received_abort_set_num.Clear(epoch, 0);
        MessageReceiveHandler::sharding_abort_set_received_ack_num.Clear(epoch, 0);

        return true;
    }

    bool MessageReceiveHandler::ClearCache() {
        res = false;
        while(cache_clear_epoch_num + 50 < EpochManager::GetLogicalEpoch()) {
            cache_clear_epoch_num_mod = cache_clear_epoch_num % ctx.kCacheMaxLength;
            for(int i = 0; i < (int)ctx.kTxnNodeNum; i ++) {
                while (!sharding_cache[cache_clear_epoch_num_mod][i]->empty()) {
                    sharding_cache[cache_clear_epoch_num_mod][i]->pop();
                }
                while (!local_txn_cache[cache_clear_epoch_num_mod][i]->empty()) {
                    local_txn_cache[cache_clear_epoch_num_mod][i]->pop();
                }
                while (!backup_cache[cache_clear_epoch_num_mod][i]->empty()) {
                    backup_cache[cache_clear_epoch_num_mod][i]->pop();
                }
                while (!insert_set_cache[cache_clear_epoch_num_mod][i]->empty()) {
                    insert_set_cache[cache_clear_epoch_num_mod][i]->pop();
                }
                while (!abort_set_cache[cache_clear_epoch_num_mod][i]->empty()) {
                    abort_set_cache[cache_clear_epoch_num_mod][i]->pop();
                }
            }
            cache_clear_epoch_num ++;
            res = true;
        }
        return res;
    }

    bool MessageReceiveHandler::ClearStaticCounters() {
        res = false;
        while(counters_clear_epoch_num + 50 < EpochManager::GetLogicalEpoch()) {
            counters_clear_epoch_num_mod = counters_clear_epoch_num % ctx.kCacheMaxLength;
            MessageReceiveHandler::sharding_should_receive_pack_num.Clear(counters_clear_epoch_num_mod, 0),
            MessageReceiveHandler::sharding_received_pack_num.Clear(counters_clear_epoch_num_mod, 0),
            MessageReceiveHandler::sharding_should_receive_txn_num.Clear(counters_clear_epoch_num_mod, 0),
            MessageReceiveHandler::sharding_received_txn_num.Clear(counters_clear_epoch_num_mod, 0),

            MessageReceiveHandler::sharding_should_enqueue_merge_queue_txn_num.Clear(counters_clear_epoch_num_mod, 0),
            MessageReceiveHandler::sharding_enqueued_merge_queue_txn_num.Clear(counters_clear_epoch_num_mod, 0),
            MessageReceiveHandler::should_enqueue_local_txn_queue_txn_num.Clear(counters_clear_epoch_num_mod, 0),
            MessageReceiveHandler::enqueued_local_txn_queue_txn_num.Clear(counters_clear_epoch_num_mod, 0),

            MessageReceiveHandler::sharding_should_handle_local_txn_num.Clear(counters_clear_epoch_num_mod, 0),
            MessageReceiveHandler::sharding_handled_local_txn_num.Clear(counters_clear_epoch_num_mod, 0),
            MessageReceiveHandler::sharding_should_send_txn_num.Clear(counters_clear_epoch_num_mod, 0),
            MessageReceiveHandler::sharding_send_txn_num.Clear(counters_clear_epoch_num_mod, 0),

            MessageReceiveHandler::backup_should_send_txn_num.Clear(counters_clear_epoch_num_mod, 0),
            MessageReceiveHandler::backup_send_txn_num.Clear(counters_clear_epoch_num_mod, 0),
            MessageReceiveHandler::backup_should_receive_pack_num.Clear(counters_clear_epoch_num_mod, 0),
            MessageReceiveHandler::backup_received_pack_num.Clear(counters_clear_epoch_num_mod, 0),
            MessageReceiveHandler::backup_should_receive_txn_num.Clear(counters_clear_epoch_num_mod, 0),
            MessageReceiveHandler::backup_received_txn_num.Clear(counters_clear_epoch_num_mod, 0),
            MessageReceiveHandler::backup_received_ack_num.Clear(counters_clear_epoch_num_mod, 0),

            MessageReceiveHandler::insert_set_should_receive_num.Clear(counters_clear_epoch_num_mod, 0),
            MessageReceiveHandler::insert_set_received_num.Clear(counters_clear_epoch_num_mod, 0),
            MessageReceiveHandler::insert_set_received_ack_num.Clear(counters_clear_epoch_num_mod, 0),

            MessageReceiveHandler::sharding_should_receive_abort_set_num.Clear(counters_clear_epoch_num_mod, 0),
            MessageReceiveHandler::sharding_received_abort_set_num.Clear(counters_clear_epoch_num_mod, 0);
            MessageReceiveHandler::sharding_abort_set_received_ack_num.Clear(counters_clear_epoch_num_mod, 0);

            counters_clear_epoch_num ++;
            res = true;
        }
        return res;
    }

}