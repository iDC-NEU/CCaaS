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

    std::vector<std::unique_ptr<BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>>
        EpochMessageReceiveHandler::epoch_remote_sharding_txn,
        EpochMessageReceiveHandler::epoch_local_sharding_txn,
        EpochMessageReceiveHandler::epoch_local_txn,
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
//        ///local sharding txn counters
//        EpochMessageReceiveHandler::sharding_should_enqueue_merge_queue_txn_num(10, 1),
//        EpochMessageReceiveHandler::sharding_enqueued_merge_queue_txn_num(10, 1),
//        EpochMessageReceiveHandler::should_enqueue_local_txn_queue_txn_num(10, 1),
//        EpochMessageReceiveHandler::enqueued_local_txn_queue_txn_num(10, 1),
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
        txn_ptr = nullptr;
//        pack_param = nullptr;
//        server_dequeue_id = 0, epoch_mod = 0;
//        epoch = 0, max_length = 0;
//        res = false, sleep_flag = false;
        thread_id = id;
        ctx = ctx_;
//        max_length = ctx_.kCacheMaxLength;
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

        epoch_remote_sharding_txn.resize(max_length);
        epoch_local_sharding_txn.resize(max_length);
        epoch_local_txn.resize(max_length);
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
            epoch_remote_sharding_txn[i] = std::make_unique<BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>();
            epoch_local_sharding_txn[i] = std::make_unique<BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>();
            epoch_local_txn[i] = std::make_unique<BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>();
            epoch_backup_txn[i] = std::make_unique<BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>();
            epoch_insert_set[i] = std::make_unique<BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>();
            epoch_abort_set[i] = std::make_unique<BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>();
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
//        sharding_should_receive_txn_num.Init(max_length, sharding_num, PACKNUM),
        sharding_received_txn_num.Init(max_length, sharding_num),

        backup_should_send_txn_num.Init(max_length, sharding_num),
        backup_send_txn_num.Init(max_length, sharding_num),
        backup_should_receive_pack_num.Init(max_length, sharding_num, 1),
        backup_received_pack_num.Init(max_length, sharding_num),
        backup_should_receive_txn_num.Init(max_length, sharding_num, 0),
//        backup_should_receive_txn_num.Init(max_length, sharding_num, PACKNUM),
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
            MessageQueue::listen_message_queue->wait_dequeue(message_ptr);
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
                Merger::CommitQueueEnqueue(ctx, message_epoch, std::move(txn_ptr));
                sharding_handled_local_txn_num.IncCount(message_epoch, thread_id, 1);
                break;
            }
            case proto::TxnType::RemoteServerTxn : {
                sharding_should_handle_remote_txn_num.IncCount(message_epoch, thread_id, 1);
                Merger::MergeQueueEnqueue(ctx, message_epoch, std::make_unique<proto::Transaction>(*txn_ptr));
                Merger::CommitQueueEnqueue(ctx, message_epoch, std::move(txn_ptr));
                sharding_received_txn_num.IncCount(message_epoch,message_server_id, 1);
                sharding_handled_remote_txn_num.IncCount(message_epoch, thread_id, 1);
                break;
            }
            case proto::TxnType::EpochEndFlag : {
                sharding_should_receive_txn_num.IncCount(message_epoch,message_server_id,txn_ptr->csn());
                sharding_received_pack_num.IncCount(message_epoch,message_server_id, 1);
                CheckEpochShardingReceiveComplete(ctx,message_epoch);
//                if(message_epoch < EpochManager::GetPhysicalEpoch() &&
//                   IsShardingPackReceiveComplete(message_epoch, message_server_id) &&
//                   IsShardingTxnReceiveComplete(message_epoch, message_server_id)) {
//                    EpochMessageSendHandler::SendTxnToServer(ctx, message_epoch,
//                                                             message_server_id, empty_txn, proto::TxnType::EpochShardingACK);
////                printf("= send sharding ack epoch, %lu server_id %lu\n", sharding_epoch, id);
//                }
//                printf("EpochEndFlag epoch %lu server %lu\n", message_epoch, message_server_id);
                break;
            }
            case proto::TxnType::BackUpTxn : {
                epoch_backup_txn[message_epoch_mod]->enqueue(std::move(txn_ptr));
                epoch_backup_txn[message_epoch_mod]->enqueue(nullptr);
                backup_received_txn_num.IncCount(message_epoch,message_server_id, 1);
                break;
            }
            case proto::TxnType::BackUpEpochEndFlag : {
                backup_should_receive_txn_num.IncCount(message_epoch,message_server_id,txn_ptr->csn());
                backup_received_pack_num.IncCount(message_epoch,message_server_id, 1);
//                if(message_epoch < EpochManager::GetPhysicalEpoch() &&
//                   IsBackUpPackReceiveComplete(message_epoch, message_server_id) &&
//                   IsBackUpTxnReceiveComplete(message_epoch, message_server_id)) {
//                    EpochMessageSendHandler::SendTxnToServer(ctx, message_epoch,
//                                                             message_server_id, empty_txn, proto::TxnType::BackUpACK);
////                printf(" == send backup ack epoch, %lu server_id %lu\n", backup_epoch, id);
//                }
//                printf("BackUpEpochEndFlag epoch %lu server %lu\n", message_epoch, message_server_id);
                break;
            }
            case proto::TxnType::AbortSet : {
                UpdateEpochAbortSet();
                epoch_abort_set[message_epoch_mod]->enqueue(std::move(txn_ptr));
                epoch_abort_set[message_epoch_mod]->enqueue(nullptr);
                abort_set_received_num.IncCount(message_epoch,message_server_id, 1);
                ///send abort set ack
                EpochMessageSendHandler::SendTxnToServer(ctx, message_epoch,
                            message_server_id, empty_txn, proto::TxnType::AbortSetACK);
//                printf("AbortSet epoch %lu server %lu\n", message_epoch, message_server_id);
                break;
            }
            case proto::TxnType::InsertSet : {
                epoch_insert_set[message_epoch_mod]->enqueue(std::move(txn_ptr));
                epoch_insert_set[message_epoch_mod]->enqueue(nullptr);
                insert_set_received_num.IncCount(message_epoch,message_server_id, 1);
                ///send insert set ack
                EpochMessageSendHandler::SendTxnToServer(ctx, message_epoch,
                            message_server_id, empty_txn, proto::TxnType::InsertSetACK);
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
        if(ctx.taas_mode == TaasMode::Sharding) {
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
                        continue;
                    }
                    else {
                        sharding_should_send_txn_num.IncCount(message_epoch, i, 1);
                        EpochMessageSendHandler::SendTxnToServer(ctx, message_epoch, i, *(sharding_row_vector[i]), proto::TxnType::RemoteServerTxn);
                        sharding_send_txn_num.IncCount(message_epoch, i, 1);
                    }
                }
            }
            if(sharding_row_vector[ctx.txn_node_ip_index]->row_size() > 0) {
                ///read version check need to wait until last epoch has committed.
                Merger::MergeQueueEnqueue(ctx, message_epoch, std::make_unique<proto::Transaction>(*txn_ptr));
//            res = Merger::EpochMerge(ctx, message_epoch, std::move(sharding_row_vector[ctx.txn_node_ip_index]));
//            if(!res) {
//                MessageSendHandler::SendTxnCommitResultToClient(ctx, *(sharding_row_vector[ctx.txn_node_ip_index]), proto::TxnState::Abort);
//            }
            }
        }
        else if(ctx.taas_mode == TaasMode::MultiMaster) {
            Merger::MergeQueueEnqueue(ctx, message_epoch, std::make_unique<proto::Transaction>(*txn_ptr));
            sharding_should_send_txn_num.IncCount(message_epoch, ctx.txn_node_ip_index, 1);
            EpochMessageSendHandler::SendTxnToServer(ctx, message_epoch, ctx.txn_node_ip_index, *(txn_ptr), proto::TxnType::RemoteServerTxn);
            sharding_send_txn_num.IncCount(message_epoch, 0, 1);
        }

        {///backup sending full txn
            backup_should_send_txn_num.IncCount(message_epoch, ctx.txn_node_ip_index, 1);
            EpochMessageSendHandler::SendTxnToServer(ctx, message_epoch, message_server_id, *(txn_ptr), proto::TxnType::BackUpTxn);
            backup_send_txn_num.IncCount(message_epoch, ctx.txn_node_ip_index, 1);

            epoch_backup_txn[message_epoch_mod]->enqueue(std::make_unique<proto::Transaction>(*txn_ptr));
            epoch_backup_txn[message_epoch_mod]->enqueue(nullptr);
        }
        return true;
    }

    bool EpochMessageReceiveHandler::UpdateEpochAbortSet() {
        message_epoch = txn_ptr->commit_epoch();
        message_epoch_mod = txn_ptr->commit_epoch() % ctx.kCacheMaxLength;
        for(int i = 0; i < txn_ptr->row_size(); i ++) {
            Merger::epoch_insert_set[message_epoch_mod]->insert(txn_ptr->row(i).key(), txn_ptr->row(i).data());
        }
        return true;
    }




    bool EpochMessageReceiveHandler::CheckReceivedStatesAndReply() {
        res = false;
        auto& id = server_reply_ack_id;
//        if(redo_log_push_down_reply < EpochManager::GetLogicalEpoch() &&
//           (Merger::IsEpochCommitComplete(ctx, redo_log_push_down_reply) || redo_log_push_down_reply < EpochManager::GetPushDownEpoch() )) {
////            EpochMessageSendHandler::SendMessageToAll(ctx, redo_log_push_down_reply, proto::TxnType::EpochLogPushDownComplete);
//            EpochMessageSendHandler::SendTxnToServer(ctx, redo_log_push_down_reply,
//                                                     server_reply_ack_id, empty_txn, proto::TxnType::EpochLogPushDownComplete);
////            LOG(INFO) << "send EpochLogPushDownComplete ack, epoch: " << redo_log_push_down_reply << ", server id:" << id;
//            redo_log_push_down_reply ++;
//            res = true;
//        }

        ///to single server  send ack
        if(id != ctx.txn_node_ip_index) {
            auto &sharding_epoch = sharding_send_ack_epoch_num[id];
            if(sharding_epoch < EpochManager::GetPhysicalEpoch() &&
               IsShardingPackReceiveComplete(sharding_epoch, id) &&
               IsShardingTxnReceiveComplete(sharding_epoch, id)) {
                EpochMessageSendHandler::SendTxnToServer(ctx, sharding_epoch,
                                                         id, empty_txn, proto::TxnType::EpochShardingACK);
//                printf("= send sharding ack epoch, %lu server_id %lu\n", sharding_epoch, id);
                sharding_epoch ++;
                res = true;
            }

            auto& backup_epoch = backup_send_ack_epoch_num[id];
            if(backup_epoch < EpochManager::GetPhysicalEpoch() &&
               IsBackUpPackReceiveComplete(backup_epoch, id) &&
               IsBackUpTxnReceiveComplete(backup_epoch, id)) {
                EpochMessageSendHandler::SendTxnToServer(ctx, backup_epoch,
                                                         id, empty_txn, proto::TxnType::BackUpACK);
//                printf(" == send backup ack epoch, %lu server_id %lu\n", backup_epoch, id);
                backup_epoch ++;
                res = true;
            }

        }
        id = (id + 1) % ctx.kTxnNodeNum;
        return res;
    }

    bool EpochMessageReceiveHandler::StaticClear(const Context& ctx, uint64_t& epoch) {
//        printf("clean receive cache epoch %lu\n", epoch);
        auto cache_clear_epoch_num_mod = epoch % ctx.kCacheMaxLength;
        sharding_should_receive_pack_num.Clear(cache_clear_epoch_num_mod, 1),///relate to server state
                sharding_received_pack_num.Clear(cache_clear_epoch_num_mod, 0),

                sharding_should_receive_txn_num.Clear(cache_clear_epoch_num_mod, 0),
//        sharding_should_receive_txn_num.Clear(cache_clear_epoch_num_mod, PACKNUM),
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
//        backup_should_receive_txn_num.Clear(cache_clear_epoch_num_mod, PACKNUM),
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

        auto txn = std::make_unique<proto::Transaction>();
        while (epoch_remote_sharding_txn[cache_clear_epoch_num_mod]->try_dequeue(txn));
        txn = std::make_unique<proto::Transaction>();
        while (epoch_local_txn[cache_clear_epoch_num_mod]->try_dequeue(txn));
        txn = std::make_unique<proto::Transaction>();
        while (epoch_backup_txn[cache_clear_epoch_num_mod]->try_dequeue(txn));
        txn = std::make_unique<proto::Transaction>();
        while (epoch_insert_set[cache_clear_epoch_num_mod]->try_dequeue(txn));
        txn = std::make_unique<proto::Transaction>();
        while (epoch_abort_set[cache_clear_epoch_num_mod]->try_dequeue(txn));
        return true;
    }

}