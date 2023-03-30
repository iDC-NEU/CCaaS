//
// Created by 周慰星 on 11/9/22.
//
#include <queue>
#include <utility>

#include "epoch/epoch_manager.h"
#include "message/message.h"
#include "message/handler_receive.h"
#include "tools/utilities.h"
#include "transaction/merge.h"


namespace Taas {
    std::vector<uint64_t>
            MessageReceiveHandler::sharding_send_ack_epoch_num,
            MessageReceiveHandler::backup_send_ack_epoch_num,
            MessageReceiveHandler::backup_insert_set_send_ack_epoch_num,
            MessageReceiveHandler::abort_set_send_ack_epoch_num; /// check and reply ack

    std::vector<std::unique_ptr<BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>>
        MessageReceiveHandler::epoch_remote_sharding_txn,
        MessageReceiveHandler::epoch_local_sharding_txn,
        MessageReceiveHandler::epoch_local_txn,
        MessageReceiveHandler::epoch_backup_txn,
        MessageReceiveHandler::epoch_insert_set,
        MessageReceiveHandler::epoch_abort_set;
    ///这里需要注意 这几个计数器是以server_id为粒度增加的，不是线程id ！！！
    AtomicCounters_Cache ///epoch, server_id, value
        ///local txn counters
        MessageReceiveHandler::sharding_should_handle_local_txn_num(10, 1),
        MessageReceiveHandler::sharding_handled_local_txn_num(10, 1),
        MessageReceiveHandler::sharding_should_send_txn_num(10, 1),
        MessageReceiveHandler::sharding_send_txn_num(10, 1),
        ///remote sharding txn counters
        MessageReceiveHandler::sharding_should_receive_pack_num(10, 1),
        MessageReceiveHandler::sharding_received_pack_num(10, 1),
        MessageReceiveHandler::sharding_should_receive_txn_num(10, 1),
        MessageReceiveHandler::sharding_received_txn_num(10, 1),
        ///local sharding txn counters
        MessageReceiveHandler::sharding_should_enqueue_merge_queue_txn_num(10, 1),
        MessageReceiveHandler::sharding_enqueued_merge_queue_txn_num(10, 1),
        MessageReceiveHandler::should_enqueue_local_txn_queue_txn_num(10, 1),
        MessageReceiveHandler::enqueued_local_txn_queue_txn_num(10, 1),
        ///sharding ack
        MessageReceiveHandler::sharding_received_ack_num(10, 1),
        ///backup txn counters
        MessageReceiveHandler::backup_should_send_txn_num(10, 1),
        MessageReceiveHandler::backup_send_txn_num(10, 1),
        MessageReceiveHandler::backup_should_receive_pack_num(10, 1),
        MessageReceiveHandler::backup_received_pack_num(10, 1),
        MessageReceiveHandler::backup_should_receive_txn_num(10, 1),
        MessageReceiveHandler::backup_received_txn_num(10, 1),
        MessageReceiveHandler::backup_received_ack_num(10, 1),
        ///insert set counters
        MessageReceiveHandler::insert_set_should_receive_num(10, 1),
        MessageReceiveHandler::insert_set_received_num(10, 1),
        MessageReceiveHandler::insert_set_received_ack_num(10, 1),
        ///abort set counters
        MessageReceiveHandler::sharding_should_receive_abort_set_num(10, 1),
        MessageReceiveHandler::sharding_received_abort_set_num(10, 1),
        MessageReceiveHandler::sharding_abort_set_received_ack_num(10, 1),
        MessageReceiveHandler::redo_log_push_down_ack_num(10, 1),
        MessageReceiveHandler::redo_log_push_down_local_epoch(10, 1);

    bool MessageReceiveHandler::Init(uint64_t id, Context context) {
        message_ptr = nullptr;
        txn_ptr = nullptr;
//        pack_param = nullptr;
        server_dequeue_id = 0, epoch_mod = 0;
//        epoch = 0, max_length = 0;
//        res = false, sleep_flag = false;
        thread_id = id;
        ctx = std::move(context);
        max_length = ctx.kCacheMaxLength;
        sharding_num = ctx.kTxnNodeNum;

        return true;
    }

    bool MessageReceiveHandler::StaticInit(const Context& context) {
        auto max_length = context.kCacheMaxLength;
        auto sharding_num = context.kTxnNodeNum;

        sharding_send_ack_epoch_num.reserve(sharding_num + 1);
        backup_send_ack_epoch_num.reserve(sharding_num + 1);
        backup_insert_set_send_ack_epoch_num.reserve(sharding_num + 1);
        abort_set_send_ack_epoch_num.reserve(sharding_num + 1);
        for(int i = 0; i <= (int) sharding_num; i ++ ) { /// start at 1, not 0
            sharding_send_ack_epoch_num.emplace_back(1);
            backup_send_ack_epoch_num.emplace_back(1);
            backup_insert_set_send_ack_epoch_num.emplace_back(1);
            abort_set_send_ack_epoch_num.emplace_back(1);
        }

        epoch_remote_sharding_txn.resize(EpochManager::max_length);
        epoch_local_sharding_txn.resize(EpochManager::max_length);
        epoch_local_txn.resize(EpochManager::max_length);
        epoch_backup_txn.resize(EpochManager::max_length);
        epoch_insert_set.resize(EpochManager::max_length);
        epoch_abort_set.resize(EpochManager::max_length);

        for(int i = 0; i < static_cast<int>(EpochManager::max_length); i ++) {
            epoch_remote_sharding_txn[i] = std::make_unique<BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>();
            epoch_local_sharding_txn[i] = std::make_unique<BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>();
            epoch_local_txn[i] = std::make_unique<BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>();
            epoch_backup_txn[i] = std::make_unique<BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>();
            epoch_insert_set[i] = std::make_unique<BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>();
            epoch_abort_set[i] = std::make_unique<BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>();
        }

        sharding_should_receive_pack_num.Init(max_length, sharding_num, 1),
        sharding_received_pack_num.Init(max_length, sharding_num),
        sharding_should_receive_txn_num.Init(max_length, sharding_num),
        sharding_received_txn_num.Init(max_length, sharding_num),

        sharding_should_enqueue_merge_queue_txn_num.Init(max_length, sharding_num),
        sharding_enqueued_merge_queue_txn_num.Init(max_length, sharding_num),
        should_enqueue_local_txn_queue_txn_num.Init(max_length, sharding_num),
        enqueued_local_txn_queue_txn_num.Init(max_length, sharding_num),

        sharding_should_handle_local_txn_num.Init(max_length, sharding_num),
        sharding_handled_local_txn_num.Init(max_length, sharding_num),
        sharding_should_send_txn_num.Init(max_length, sharding_num),
        sharding_send_txn_num.Init(max_length, sharding_num),

        sharding_received_ack_num.Init(max_length, sharding_num),

        backup_should_send_txn_num.Init(max_length, sharding_num),
        backup_send_txn_num.Init(max_length, sharding_num),
        backup_should_receive_pack_num.Init(max_length, sharding_num, 1),
        backup_received_pack_num.Init(max_length, sharding_num),
        backup_should_receive_txn_num.Init(max_length, sharding_num),
        backup_received_txn_num.Init(max_length, sharding_num),
        backup_received_ack_num.Init(max_length, sharding_num),


        insert_set_should_receive_num.Init(max_length, sharding_num, 1),
        insert_set_received_num.Init(max_length, sharding_num),
        insert_set_received_ack_num.Init(max_length, sharding_num),

        sharding_should_receive_abort_set_num.Init(max_length, sharding_num, 1),
        sharding_received_abort_set_num.Init(max_length, sharding_num);
        sharding_abort_set_received_ack_num.Init(max_length, sharding_num);

        redo_log_push_down_ack_num.Init(max_length, sharding_num);
        redo_log_push_down_local_epoch.Init(max_length, sharding_num);

        return true;
    }

    bool MessageReceiveHandler::StaticClear() {
        res = false;
        while(cache_clear_epoch_num + 50 < EpochManager::GetLogicalEpoch()) {
            cache_clear_epoch_num_mod = cache_clear_epoch_num % ctx.kCacheMaxLength;
            sharding_should_receive_pack_num.Clear(cache_clear_epoch_num_mod, 1),///relate to server state
            sharding_received_pack_num.Clear(cache_clear_epoch_num_mod, 0),
            sharding_should_receive_txn_num.Clear(cache_clear_epoch_num_mod, 0),
            sharding_received_txn_num.Clear(cache_clear_epoch_num_mod, 0),

            sharding_should_enqueue_merge_queue_txn_num.Clear(cache_clear_epoch_num_mod, 0),
            sharding_enqueued_merge_queue_txn_num.Clear(cache_clear_epoch_num_mod, 0),
            should_enqueue_local_txn_queue_txn_num.Clear(cache_clear_epoch_num_mod, 0),
            enqueued_local_txn_queue_txn_num.Clear(cache_clear_epoch_num_mod, 0),

            sharding_should_handle_local_txn_num.Clear(cache_clear_epoch_num_mod, 0),
            sharding_handled_local_txn_num.Clear(cache_clear_epoch_num_mod, 0),
            sharding_should_send_txn_num.Clear(cache_clear_epoch_num_mod, 0),
            sharding_send_txn_num.Clear(cache_clear_epoch_num_mod, 0),

            sharding_received_ack_num.Clear(cache_clear_epoch_num_mod, 0),

            backup_should_send_txn_num.Clear(cache_clear_epoch_num_mod, 0),
            backup_send_txn_num.Clear(cache_clear_epoch_num_mod, 0),

            backup_should_receive_pack_num.Clear(cache_clear_epoch_num_mod, 1),///relate to server state
            backup_received_pack_num.Clear(cache_clear_epoch_num_mod, 0),
            backup_should_receive_txn_num.Clear(cache_clear_epoch_num_mod, 0),
            backup_received_txn_num.Clear(cache_clear_epoch_num_mod, 0),
            backup_received_ack_num.Clear(cache_clear_epoch_num_mod, 0),

            insert_set_should_receive_num.Clear(cache_clear_epoch_num_mod, 1),///relate to server state
            insert_set_received_num.Clear(cache_clear_epoch_num_mod, 0),
            insert_set_received_ack_num.Clear(cache_clear_epoch_num_mod, 0),

            sharding_should_receive_abort_set_num.Clear(cache_clear_epoch_num_mod, 1),///relate to server state
            sharding_received_abort_set_num.Clear(cache_clear_epoch_num_mod, 0);
            sharding_abort_set_received_ack_num.Clear(cache_clear_epoch_num_mod, 0);

            redo_log_push_down_ack_num.Clear(cache_clear_epoch_num_mod, 0);
            redo_log_push_down_local_epoch.Clear(cache_clear_epoch_num_mod, 0);


            while (!epoch_remote_sharding_txn[cache_clear_epoch_num_mod]->try_dequeue(txn_ptr));
            while (!epoch_local_txn[cache_clear_epoch_num_mod]->try_dequeue(txn_ptr));
            while (!epoch_backup_txn[cache_clear_epoch_num_mod]->try_dequeue(txn_ptr));
            while (!epoch_insert_set[cache_clear_epoch_num_mod]->try_dequeue(txn_ptr));
            while (!epoch_abort_set[cache_clear_epoch_num_mod]->try_dequeue(txn_ptr));

            cache_clear_epoch_num ++;
            res = true;
        }
        return res;
    }





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
                    Merger::epoch_should_merge_txn_num.IncCount(message_epoch, i, 1);
                    Merger::merge_queue->enqueue(std::move(sharding_row_vector[i]));
                    Merger::merge_queue->enqueue(nullptr);
                    sharding_enqueued_merge_queue_txn_num.IncCount(message_epoch, i, 1);
                }
                else {
                    sharding_should_send_txn_num.IncCount(message_epoch, i, 1);
                    MessageSendHandler::SendTxnToServer(ctx, message_epoch,
                                                        i, *(sharding_row_vector[i]), proto::TxnType::RemoteServerTxn);
                    MessageReceiveHandler::sharding_send_txn_num.IncCount(message_epoch, i, 1);
                    sharding_send_txn_num.IncCount(message_epoch, i, 1);
                }
            }
        }
        {///backup sending full txn
            backup_should_send_txn_num.IncCount(message_epoch, ctx.txn_node_ip_index, 1);
            MessageSendHandler::SendTxnToServer(ctx, message_epoch,
                                                message_server_id, *(txn_ptr), proto::TxnType::BackUpTxn);
            backup_send_txn_num.IncCount(message_epoch, ctx.txn_node_ip_index, 1);

            should_enqueue_local_txn_queue_txn_num.IncCount(epoch_mod, ctx.txn_node_ip_index, 1);
            epoch_backup_txn[message_epoch_mod]->enqueue(std::make_unique<proto::Transaction>(*txn_ptr));
            epoch_backup_txn[message_epoch_mod]->enqueue(nullptr);
            enqueued_local_txn_queue_txn_num.IncCount(epoch_mod, ctx.txn_node_ip_index, 1);
        }
        sharding_handled_local_txn_num.IncCount(message_epoch, thread_id, 1);
        return true;
    }

    bool MessageReceiveHandler::UpdateEpochAbortSet() {
        message_epoch = txn_ptr->commit_epoch();
        message_epoch_mod = txn_ptr->commit_epoch() % ctx.kCacheMaxLength;
        for(int i = 0; i < txn_ptr->row_size(); i ++) {
            Merger::epoch_insert_set[message_epoch_mod]->insert(txn_ptr->row(i).key(), txn_ptr->row(i).data());
        }
        return true;
    }

    bool MessageReceiveHandler::SetMessageRelatedCountersInfo() {
        message_epoch = txn_ptr->commit_epoch();
        message_epoch_mod = txn_ptr->commit_epoch() % ctx.kCacheMaxLength;
        message_server_id = txn_ptr->server_id();
        message_sharding_id = txn_ptr->sharding_id();
        return true;
    }

    bool MessageReceiveHandler::HandleReceivedTxn() {
        SetMessageRelatedCountersInfo();
        ///这里需要注意 这几个计数器是以server_id为粒度增加的，不是线程id ！！！

        switch (txn_ptr->txn_type()) {
            case proto::TxnType::ClientTxn : {/// sql node --> txn node
                txn_ptr->set_commit_epoch(EpochManager::GetPhysicalEpoch());
                txn_ptr->set_csn(now_to_us());
                txn_ptr->set_server_id(ctx.txn_node_ip_index);
                SetMessageRelatedCountersInfo();
                should_enqueue_local_txn_queue_txn_num.IncCount(message_epoch, ctx.txn_node_ip_index, 1);
                Merger::epoch_should_commit_txn_num.IncCount(message_epoch, ctx.txn_node_ip_index, 1);
                Sharding();
                assert(txn_ptr != nullptr);
                Merger::epoch_local_txn_queue[message_epoch_mod]->enqueue(std::move(txn_ptr));
                Merger::epoch_local_txn_queue[message_epoch_mod]->enqueue(nullptr);
                enqueued_local_txn_queue_txn_num.IncCount(message_epoch, ctx.txn_node_ip_index, 1);

                break;
            }
            case proto::TxnType::RemoteServerTxn : {
                if ((EpochManager::GetLogicalEpoch() % ctx.kCacheMaxLength) ==
                    ((message_epoch_mod + 55) % ctx.kCacheMaxLength))
                    assert(false);
                sharding_should_enqueue_merge_queue_txn_num.IncCount(message_epoch, message_server_id, 1);
                Merger::epoch_should_merge_txn_num.IncCount(message_epoch, message_server_id, 1);
                Merger::merge_queue->enqueue(std::move(txn_ptr));
                Merger::merge_queue->enqueue(nullptr);
                sharding_received_txn_num.IncCount(message_epoch,message_server_id, 1);
                sharding_should_enqueue_merge_queue_txn_num.IncCount(message_epoch, message_server_id, 1);
                break;
            }
            case proto::TxnType::EpochEndFlag : {
                sharding_should_receive_txn_num.IncCount(message_epoch,message_server_id,txn_ptr->csn());
                sharding_received_pack_num.IncCount(message_epoch,message_server_id, 1);
                break;
            }
            case proto::TxnType::BackUpTxn : {
                if ((EpochManager::GetLogicalEpoch() % ctx.kCacheMaxLength) ==
                    ((message_epoch_mod + 55) % ctx.kCacheMaxLength))
                    assert(false);
                epoch_backup_txn[message_epoch_mod]->enqueue(std::move(txn_ptr));
                epoch_backup_txn[message_epoch_mod]->enqueue(nullptr);
                backup_received_txn_num.IncCount(message_epoch,message_server_id, 1);
                break;
            }
            case proto::TxnType::BackUpEpochEndFlag : {
                backup_should_receive_txn_num.IncCount(message_epoch,message_server_id,txn_ptr->csn());
                backup_received_pack_num.IncCount(message_epoch,message_server_id, 1);
                break;
            }
            case proto::TxnType::AbortSet : {
                UpdateEpochAbortSet();
                epoch_abort_set[message_epoch_mod]->enqueue(std::move(txn_ptr));
                epoch_abort_set[message_epoch_mod]->enqueue(nullptr);
                sharding_received_abort_set_num.IncCount(message_epoch,message_server_id, 1);
                break;
            }
            case proto::TxnType::InsertSet : {
                epoch_insert_set[message_epoch_mod]->enqueue(std::move(txn_ptr));
                epoch_insert_set[message_epoch_mod]->enqueue(nullptr);
                insert_set_received_num.IncCount(message_epoch,message_server_id, 1);
                break;
            }
            case proto::TxnType::EpochShardingACK : {
                sharding_received_ack_num.IncCount(message_epoch,message_server_id, 1);
                break;
            }
            case proto::TxnType::BackUpACK : {
                backup_received_ack_num.IncCount(message_epoch,message_server_id, 1);
                break;
            }
            case proto::TxnType::AbortSetACK : {
                sharding_abort_set_received_ack_num.IncCount(message_epoch,message_server_id, 1);
                break;
            }
            case proto::TxnType::InsertSetACK : {
                insert_set_received_ack_num.IncCount(message_epoch,message_server_id, 1);
                break;
            }
            case proto::TxnType::EpochLogPushDownComplete : {
                redo_log_push_down_ack_num.IncCount(message_epoch,message_server_id, 1);
                break;
            }
            case proto::NullMark:
            case proto::TxnType_INT_MIN_SENTINEL_DO_NOT_USE_:
            case proto::TxnType_INT_MAX_SENTINEL_DO_NOT_USE_:
            case proto::CommittedTxn:
                break;
        }


        return true;
    }


    bool MessageReceiveHandler::HandleReceivedMessage() {
        sleep_flag = false;
        while (listen_message_queue->try_dequeue(message_ptr)) {
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
                request_queue->enqueue(std::move(msg_ptr));
                request_queue->enqueue(nullptr);
            }
            sleep_flag = true;
        }
        return sleep_flag;
    }

    bool MessageReceiveHandler::CheckReceivedStatesAndReply() {
        res = false;
        auto& id = server_reply_ack_id;
        ///to all server
        /// change epoch_record_committed_txn_num to tikv check
        if(Merger::epoch_record_committed_txn_num.GetCount(redo_log_push_down_reply) >=
                Merger::epoch_record_commit_txn_num.GetCount(redo_log_push_down_reply) &&
                redo_log_push_down_reply < EpochManager::GetLogicalEpoch()) {
            MessageSendHandler::SendTxnToServer(ctx, redo_log_push_down_reply,
                                                server_reply_ack_id, empty_txn, proto::TxnType::EpochLogPushDownComplete);
            redo_log_push_down_reply ++;
        }
        ///to single server  send ack
        if(id != ctx.txn_node_ip_index) {
            auto &sharding_epoch = sharding_send_ack_epoch_num[id];
            if(sharding_received_pack_num.GetCount(sharding_epoch, id) >=
                sharding_should_receive_pack_num.GetCount(sharding_epoch, id) &&
                    sharding_received_txn_num.GetCount(sharding_epoch, id) >=
                    sharding_should_receive_txn_num.GetCount(sharding_epoch, id) ) {
                MessageSendHandler::SendTxnToServer(ctx, sharding_epoch,
                                                    id, empty_txn, proto::TxnType::EpochShardingACK);
                sharding_epoch ++;
                res = true;
            }

            auto& backup_epoch = backup_send_ack_epoch_num[id];
            if(backup_received_pack_num.GetCount(backup_epoch, id) >=
                backup_should_receive_pack_num.GetCount(backup_epoch, id)&&
                    backup_received_txn_num.GetCount(backup_epoch, id) >=
                    backup_should_receive_txn_num.GetCount(backup_epoch, id) ) {
                MessageSendHandler::SendTxnToServer(ctx, backup_epoch,
                                                        id, empty_txn, proto::TxnType::BackUpACK);
                backup_epoch ++;
                res = true;
            }

            auto& backup_insert_epoch = backup_insert_set_send_ack_epoch_num[id];
            if(sharding_received_abort_set_num.GetCount(backup_insert_epoch, id) >=
                insert_set_should_receive_num.GetCount(sharding_epoch, id)) {
                MessageSendHandler::SendTxnToServer(ctx, backup_insert_epoch,
                                                        id, empty_txn, proto::TxnType::AbortSetACK);
                backup_insert_epoch ++;
                res = true;
            }

            auto& abort_set_epoch = abort_set_send_ack_epoch_num[id];
            if(insert_set_received_num.GetCount(abort_set_epoch, id) >=
                sharding_should_receive_abort_set_num.GetCount(sharding_epoch, id)) {
                MessageSendHandler::SendTxnToServer(ctx, abort_set_epoch,
                                                        id, empty_txn, proto::TxnType::InsertSetACK);
                abort_set_epoch ++;
                res = true;
            }
        }
        id = (id + 1) % ctx.kTxnNodeNum;
        return res;
    }




}