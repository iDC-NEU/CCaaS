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


namespace Taas {
//    const uint64_t PACKNUM = 1L<<32;///
    std::vector<uint64_t>
            EpochMessageReceiveHandler::sharding_send_ack_epoch_num,
            EpochMessageReceiveHandler::backup_send_ack_epoch_num,
            EpochMessageReceiveHandler::backup_insert_set_send_ack_epoch_num,
            EpochMessageReceiveHandler::abort_set_send_ack_epoch_num; /// check and reply ack

    std::vector<std::unique_ptr<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>>
        EpochMessageReceiveHandler::epoch_backup_txn,
        EpochMessageReceiveHandler::epoch_insert_set,
        EpochMessageReceiveHandler::epoch_abort_set;

    std::vector<std::unique_ptr<std::atomic<bool>>>
            EpochMessageReceiveHandler::epoch_sharding_send_complete,
            EpochMessageReceiveHandler::epoch_sharding_receive_complete,
            EpochMessageReceiveHandler::epoch_back_up_complete,
            EpochMessageReceiveHandler::epoch_abort_set_merge_complete,
            EpochMessageReceiveHandler::epoch_insert_set_complete;

    ///这里需要注意 这几个计数器是以server_id为粒度增加的，不是线程id ！！！
    AtomicCounters_Cache ///epoch, server_id, value
        ///epoch txn counters
        EpochMessageReceiveHandler::sharding_should_handle_local_txn_num(10, 1), EpochMessageReceiveHandler::sharding_handled_local_txn_num(10, 1),
        EpochMessageReceiveHandler::sharding_should_handle_remote_txn_num(10, 1), EpochMessageReceiveHandler::sharding_handled_remote_txn_num(10, 1),
        ///local txn counters
        EpochMessageReceiveHandler::sharding_should_send_txn_num(10, 1),
        EpochMessageReceiveHandler::sharding_send_txn_num(10, 1),
        ///remote sharding txn counters
        EpochMessageReceiveHandler::sharding_should_receive_pack_num(10, 1),
        EpochMessageReceiveHandler::sharding_received_pack_num(10, 1),
        EpochMessageReceiveHandler::sharding_should_receive_txn_num(10, 1),
        EpochMessageReceiveHandler::sharding_received_txn_num(10, 1),
        ///sharding ack
        EpochMessageReceiveHandler::sharding_received_ack_num(10, 1),
        ///backup send counters
        EpochMessageReceiveHandler::backup_should_send_txn_num(10, 1),
        EpochMessageReceiveHandler::backup_send_txn_num(10, 1),
        EpochMessageReceiveHandler::backup_received_ack_num(10, 1),
        ///backup receive
        EpochMessageReceiveHandler::backup_should_receive_pack_num(10, 1),
        EpochMessageReceiveHandler::backup_received_pack_num(10, 1),
        EpochMessageReceiveHandler::backup_should_receive_txn_num(10, 1),
        EpochMessageReceiveHandler::backup_received_txn_num(10, 1),

        ///insert set counters
        EpochMessageReceiveHandler::insert_set_should_receive_num(10, 1),
        EpochMessageReceiveHandler::insert_set_received_num(10, 1),
        EpochMessageReceiveHandler::insert_set_received_ack_num(10, 1),
        ///abort set counters
        EpochMessageReceiveHandler::abort_set_should_receive_num(10, 1),
        EpochMessageReceiveHandler::abort_set_received_num(10, 1),
        EpochMessageReceiveHandler::abort_set_received_ack_num(10, 1),
        ///redo log
        EpochMessageReceiveHandler::redo_log_push_down_ack_num(10, 1),
        EpochMessageReceiveHandler::redo_log_push_down_local_epoch(10, 1);



    bool EpochMessageReceiveHandler::Init(const Context& ctx_, const uint64_t &id) {
        message_ptr = nullptr;
        txn_ptr.reset();
        thread_id = id;
        ctx = ctx_;
        sharding_num = ctx_.kTxnNodeNum;

        return true;
    }

    bool EpochMessageReceiveHandler::StaticInit(const Context& context) {
        auto max_length = context.kCacheMaxLength;
        auto sharding_num = context.kTxnNodeNum;

        sharding_send_ack_epoch_num.resize(sharding_num + 1);
        backup_send_ack_epoch_num.resize(sharding_num + 1);
        backup_insert_set_send_ack_epoch_num.resize(sharding_num + 1);
        abort_set_send_ack_epoch_num.resize(sharding_num + 1);
        for(int i = 0; i <= (int) sharding_num; i ++ ) { /// start at 1, not 0
            sharding_send_ack_epoch_num[i] = 1;
            backup_send_ack_epoch_num[i] = 1;
            backup_insert_set_send_ack_epoch_num[i] = 1;
            abort_set_send_ack_epoch_num[i] = 1;
        }

        epoch_backup_txn.resize(max_length);
        epoch_insert_set.resize(max_length);
        epoch_abort_set.resize(max_length);
        epoch_sharding_send_complete.resize(max_length);
        epoch_sharding_receive_complete.resize(max_length);
        epoch_back_up_complete.resize(max_length);
        epoch_abort_set_merge_complete.resize(max_length);
        epoch_insert_set_complete.resize(max_length);

        for(int i = 0; i < static_cast<int>(max_length); i ++) {
            epoch_sharding_send_complete[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_sharding_receive_complete[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_back_up_complete[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_abort_set_merge_complete[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_insert_set_complete[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_backup_txn[i] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
            epoch_insert_set[i] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
            epoch_abort_set[i] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
        }


        sharding_should_handle_local_txn_num.Init(max_length, sharding_num),sharding_handled_local_txn_num.Init(max_length, sharding_num),
        sharding_should_handle_remote_txn_num.Init(max_length, sharding_num),sharding_handled_remote_txn_num.Init(max_length, sharding_num),
                ///local txn counters

        sharding_should_send_txn_num.Init(max_length, sharding_num),
        sharding_send_txn_num.Init(max_length, sharding_num),
        sharding_received_ack_num.Init(max_length, sharding_num),

        sharding_should_receive_pack_num.Init(max_length, sharding_num, 1),
        sharding_received_pack_num.Init(max_length, sharding_num),
        sharding_should_receive_txn_num.Init(max_length, sharding_num, 0),
        sharding_received_txn_num.Init(max_length, sharding_num),

        backup_should_send_txn_num.Init(max_length, sharding_num),
        backup_send_txn_num.Init(max_length, sharding_num),
        backup_should_receive_pack_num.Init(max_length, sharding_num, 1),
        backup_received_pack_num.Init(max_length, sharding_num),
        backup_should_receive_txn_num.Init(max_length, sharding_num, 0),
        backup_received_txn_num.Init(max_length, sharding_num),
        backup_received_ack_num.Init(max_length, sharding_num),


        insert_set_should_receive_num.Init(max_length, sharding_num, 1),
        insert_set_received_num.Init(max_length, sharding_num),
        insert_set_received_ack_num.Init(max_length, sharding_num),

        abort_set_should_receive_num.Init(max_length, sharding_num, 1),
        abort_set_received_num.Init(max_length, sharding_num);
        abort_set_received_ack_num.Init(max_length, sharding_num);

        redo_log_push_down_ack_num.Init(max_length, sharding_num);
        redo_log_push_down_local_epoch.Init(max_length, sharding_num);

        return true;
    }

    void EpochMessageReceiveHandler::HandleReceivedMessage() {
        while(!EpochManager::IsTimerStop()) {
            MessageQueue::listen_message_txn_queue->wait_dequeue(message_ptr);
            if (message_ptr == nullptr || message_ptr->empty()) continue;
            message_string_ptr = std::make_unique<std::string>(static_cast<const char *>(message_ptr->data()),
                                                               message_ptr->size());
            msg_ptr = std::make_unique<proto::Message>();
            res = UnGzip(msg_ptr.get(), message_string_ptr.get());
            assert(res);
            txn_ptr = std::make_shared<proto::Transaction>(msg_ptr->txn());
            HandleReceivedTxn();
            txn_ptr.reset();
        }
    }

    void EpochMessageReceiveHandler::HandleReceivedControlMessage() {
        while(!EpochManager::IsTimerStop()) {
            MessageQueue::listen_message_epoch_queue->wait_dequeue(message_ptr);
            if (message_ptr == nullptr || message_ptr->empty()) continue;
            message_string_ptr = std::make_unique<std::string>(static_cast<const char *>(message_ptr->data()),
                                                               message_ptr->size());
            msg_ptr = std::make_unique<proto::Message>();
            res = UnGzip(msg_ptr.get(), message_string_ptr.get());
            assert(res);
            txn_ptr = std::make_shared<proto::Transaction>(msg_ptr->txn());
            HandleReceivedTxn();
            txn_ptr.reset();
        }
    }

    bool EpochMessageReceiveHandler::SetMessageRelatedCountersInfo() {
        message_epoch = txn_ptr->commit_epoch();
        message_epoch_mod = txn_ptr->commit_epoch() % ctx.kCacheMaxLength;
        message_server_id = txn_ptr->server_id();
        txn_ptr->sharding_id();
        return true;
    }

    bool EpochMessageReceiveHandler::HandleReceivedTxn() {
        SetMessageRelatedCountersInfo();
        switch (txn_ptr->txn_type()) {
            ///这里需要注意 这几个计数器是以server_id为粒度增加的，不是线程id ！！！
            case proto::TxnType::ClientTxn : {/// sql node --> txn node
                message_epoch = EpochManager::GetPhysicalEpoch();
                sharding_should_handle_local_txn_num.IncCount(message_epoch, thread_id, 1);
                txn_ptr->set_commit_epoch(message_epoch);
                txn_ptr->set_csn(now_to_us());
                txn_ptr->set_server_id(ctx.txn_node_ip_index);
                SetMessageRelatedCountersInfo();
                HandleClientTxn();
                assert(txn_ptr != nullptr);
                Merger::CommitQueueEnqueue(ctx, message_epoch, txn_ptr);
                sharding_handled_local_txn_num.IncCount(message_epoch, thread_id, 1);
                break;
            }
            case proto::TxnType::RemoteServerTxn : {
                sharding_should_handle_remote_txn_num.IncCount(message_epoch, thread_id, 1);
                Merger::MergeQueueEnqueue(ctx, message_epoch, txn_ptr);
                Merger::CommitQueueEnqueue(ctx, message_epoch, txn_ptr);
                sharding_received_txn_num.IncCount(message_epoch,message_server_id, 1);
                if(ctx.taas_mode == TaasMode::MultiMaster) {
//                    epoch_backup_txn[message_epoch_mod]->enqueue(txn_ptr);
//                    epoch_backup_txn[message_epoch_mod]->enqueue(nullptr);
                    backup_received_txn_num.IncCount(message_epoch,message_server_id, 1);
                }
                sharding_handled_remote_txn_num.IncCount(message_epoch, thread_id, 1);
                break;
            }
            case proto::TxnType::EpochEndFlag : {
                sharding_should_receive_txn_num.IncCount(message_epoch,message_server_id,txn_ptr->csn());
                sharding_received_pack_num.IncCount(message_epoch,message_server_id, 1);
                CheckEpochShardingReceiveComplete(ctx,message_epoch);
                EpochMessageSendHandler::SendTxnToServer(message_epoch,message_server_id, empty_txn_ptr, proto::TxnType::EpochShardingACK);
                break;
            }
            case proto::TxnType::BackUpTxn : {
//                epoch_backup_txn[message_epoch_mod]->enqueue(txn_ptr);
//                epoch_backup_txn[message_epoch_mod]->enqueue(nullptr);
                backup_received_txn_num.IncCount(message_epoch,message_server_id, 1);
                break;
            }
            case proto::TxnType::BackUpEpochEndFlag : {
                backup_should_receive_txn_num.IncCount(message_epoch,message_server_id,txn_ptr->csn());
                backup_received_pack_num.IncCount(message_epoch,message_server_id, 1);
                EpochMessageSendHandler::SendTxnToServer(message_epoch,message_server_id, empty_txn_ptr, proto::TxnType::BackUpACK);
                break;
            }
            case proto::TxnType::AbortSet : {
                UpdateEpochAbortSet();
//                epoch_abort_set[message_epoch_mod]->enqueue(txn_ptr);
//                epoch_abort_set[message_epoch_mod]->enqueue(nullptr);
                abort_set_received_num.IncCount(message_epoch,message_server_id, 1);
                ///send abort set ack
                EpochMessageSendHandler::SendTxnToServer(message_epoch,message_server_id, empty_txn_ptr, proto::TxnType::AbortSetACK);
                break;
            }
            case proto::TxnType::InsertSet : {
//                epoch_insert_set[message_epoch_mod]->enqueue(txn_ptr);
//                epoch_insert_set[message_epoch_mod]->enqueue(nullptr);
                insert_set_received_num.IncCount(message_epoch,message_server_id, 1);
                ///send insert set ack
                EpochMessageSendHandler::SendTxnToServer(message_epoch, message_server_id, empty_txn_ptr, proto::TxnType::InsertSetACK);
                break;
            }
            case proto::TxnType::EpochShardingACK : {
                sharding_received_ack_num.IncCount(message_epoch,message_server_id, 1);
                CheckEpochShardingSendComplete(ctx, message_epoch);
//                printf("EpochShardingACK epoch %lu server %lu\n", message_epoch, message_server_id);
                break;
            }
            case proto::TxnType::BackUpACK : {
                backup_received_ack_num.IncCount(message_epoch,message_server_id, 1);
//                printf("=== receive backup ack from %lu epoch %lu\n", message_server_id, message_epoch);
                CheckEpochBackUpComplete(ctx, message_epoch);
//                printf("BackUpACK epoch %lu server %lu\n", message_epoch, message_server_id);
                break;
            }
            case proto::TxnType::AbortSetACK : {
                abort_set_received_ack_num.IncCount(message_epoch,message_server_id, 1);
                CheckEpochAbortSetMergeComplete(ctx, message_epoch);
//                LOG(INFO) << "=== Receive Abort Set ACK epoch :" << message_epoch  << "===\n";
//                printf("AbortSetACK epoch %lu server %lu\n", message_epoch, message_server_id);
                break;
            }
            case proto::TxnType::InsertSetACK : {
                insert_set_received_ack_num.IncCount(message_epoch,message_server_id, 1);
                break;
            }
            case proto::TxnType::EpochLogPushDownComplete : {
                redo_log_push_down_ack_num.IncCount(message_epoch,message_server_id, 1);
//                LOG(INFO) << "receive EpochLogPushDownComplete, epoch:" << message_epoch << ", server_id: " << message_server_id;
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

    bool EpochMessageReceiveHandler::HandleClientTxn() {
//        auto time1 = now_to_us();
        if(ctx.taas_mode == TaasMode::Sharding) {
            std::vector<std::shared_ptr<proto::Transaction>> sharding_row_vector;
            for(uint64_t i = 0; i < sharding_num; i ++) {
                sharding_row_vector.emplace_back(std::make_shared<proto::Transaction>());
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
                        Merger::MergeQueueEnqueue(ctx, message_epoch, sharding_row_vector[ctx.txn_node_ip_index]);
                    }
                    else {
                        sharding_should_send_txn_num.IncCount(message_epoch, i, 1);
                        EpochMessageSendHandler::SendTxnToServer(message_epoch, i, sharding_row_vector[i], proto::TxnType::RemoteServerTxn);
                        sharding_send_txn_num.IncCount(message_epoch, i, 1);
                    }
                }
            }
            backup_should_send_txn_num.IncCount(message_epoch, ctx.txn_node_ip_index, 1);
            EpochMessageSendHandler::SendTxnToServer(message_epoch, message_server_id, txn_ptr, proto::TxnType::BackUpTxn);
            backup_send_txn_num.IncCount(message_epoch, ctx.txn_node_ip_index, 1);
//            epoch_backup_txn[message_epoch_mod]->enqueue(txn_ptr);
//            epoch_backup_txn[message_epoch_mod]->enqueue(nullptr);
            sharding_row_vector.clear();
        }

        else if(ctx.taas_mode == TaasMode::MultiMaster) {
            Merger::MergeQueueEnqueue(ctx, message_epoch, txn_ptr);
            sharding_should_send_txn_num.IncCount(message_epoch, ctx.txn_node_ip_index, 1);
            backup_should_send_txn_num.IncCount(message_epoch, ctx.txn_node_ip_index, 1);
            EpochMessageSendHandler::SendTxnToServer(message_epoch, ctx.txn_node_ip_index, txn_ptr, proto::TxnType::RemoteServerTxn);
            sharding_send_txn_num.IncCount(message_epoch, 0, 1);
            backup_send_txn_num.IncCount(message_epoch, ctx.txn_node_ip_index, 1);
//            epoch_backup_txn[message_epoch_mod]->enqueue(txn_ptr);
//            epoch_backup_txn[message_epoch_mod]->enqueue(nullptr);
        }
//        auto time3 = now_to_us();
//        LOG(INFO) << "Handle Client Txn Time Cost : " << time3 - time1;
        return true;
    }

    bool EpochMessageReceiveHandler::UpdateEpochAbortSet() {
        message_epoch = txn_ptr->commit_epoch();
        message_epoch_mod = txn_ptr->commit_epoch() % ctx.kCacheMaxLength;
        for(int i = 0; i < txn_ptr->row_size(); i ++) {
            Merger::epoch_abort_txn_set[message_epoch_mod]->insert(txn_ptr->row(i).key(), txn_ptr->row(i).data());
        }
        return true;
    }

    bool EpochMessageReceiveHandler::StaticClear(const Context& ctx, uint64_t& epoch) {
        auto cache_clear_epoch_num_mod = epoch % ctx.kCacheMaxLength;
        sharding_should_receive_pack_num.Clear(cache_clear_epoch_num_mod, 1),///relate to server state
        sharding_received_pack_num.Clear(cache_clear_epoch_num_mod, 0),
        sharding_should_receive_txn_num.Clear(cache_clear_epoch_num_mod, 0),
        sharding_received_txn_num.Clear(cache_clear_epoch_num_mod, 0),
        sharding_should_handle_local_txn_num.Clear(cache_clear_epoch_num_mod, 0),
        sharding_handled_local_txn_num.Clear(cache_clear_epoch_num_mod, 0),
        sharding_should_handle_remote_txn_num.Clear(cache_clear_epoch_num_mod, 0),
        sharding_handled_remote_txn_num.Clear(cache_clear_epoch_num_mod, 0),
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
        abort_set_should_receive_num.Clear(cache_clear_epoch_num_mod, 1),///relate to server state
        abort_set_received_num.Clear(cache_clear_epoch_num_mod, 0);
        abort_set_received_ack_num.Clear(cache_clear_epoch_num_mod, 0);
        redo_log_push_down_ack_num.Clear(cache_clear_epoch_num_mod, 0);
        redo_log_push_down_local_epoch.Clear(cache_clear_epoch_num_mod, 0);
        epoch_sharding_send_complete[cache_clear_epoch_num_mod]->store(false);
        epoch_sharding_receive_complete[cache_clear_epoch_num_mod]->store(false);
        epoch_back_up_complete[cache_clear_epoch_num_mod]->store(false);
        epoch_abort_set_merge_complete[cache_clear_epoch_num_mod]->store(false);
        epoch_insert_set_complete[cache_clear_epoch_num_mod]->store(false);

//        epoch_backup_txn[cache_clear_epoch_num_mod] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
//        epoch_insert_set[cache_clear_epoch_num_mod] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
//        epoch_abort_set[cache_clear_epoch_num_mod] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();

        return true;
    }

}