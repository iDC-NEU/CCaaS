//
// Created by 周慰星 on 11/9/22.
//
#include "message/handler_send.h"
#include "message/handler_receive.h"
#include "tools/utilities.h"
#include "transaction/merge.h"

namespace Taas {

/**
 * @brief 将txn设置事务状态，并通过protobuf将Reply序列化，将序列化的结果放到send_to_client_queue中，等待发送给客户端
 *
 * @param ctx XML文件的配置信息
 * @param txn 等待回复给client的事务
 * @param txn_state 告诉client此txn的状态(Success or Abort)
 */
    bool MessageSendHandler::SendTxnCommitResultToClient(Context &ctx, proto::Transaction &txn, proto::TxnState txn_state) {
//        return true; ///test
        //不是本地事务不进行回复
        if(txn.server_id() != ctx.txn_node_ip_index) return true;

        // 设置txn的状态并创建proto对象
        txn.set_txn_state(txn_state);
        auto msg = std::make_unique<proto::Message>();
        auto rep = msg->mutable_reply_txn_result_to_client();
        rep->set_txn_state(txn_state);
        rep->set_client_txn_id(txn.client_txn_id());

        // 将Transaction使用protobuf进行序列化，序列化的结果在serialized_txn_str_ptr中
        auto serialized_txn_str_ptr = std::make_unique<std::string>();
        auto res = Gzip(msg.get(), serialized_txn_str_ptr.get());
        assert(res);

        // 将序列化的Transaction放到send_to_client_queue中，等待发送给client
        send_to_client_queue->enqueue(std::make_unique<send_params>(txn.client_txn_id(), txn.csn(), txn.client_ip(), txn.commit_epoch(), proto::TxnType::CommittedTxn, std::move(serialized_txn_str_ptr), nullptr));
        send_to_client_queue->enqueue(std::make_unique<send_params>(0, 0, "", 0, proto::TxnType::NullMark, nullptr, nullptr));
        return true;
    }

    bool MessageSendHandler::SendTxnToServer(Context& ctx, uint64_t &epoch, uint64_t &to_whom, proto::Transaction &txn, proto::TxnType txn_type) {
        auto pack_param = std::make_unique<pack_params>(to_whom, 0, "", epoch, txn_type, nullptr);
        switch (txn_type) {
            case proto::TxnType::RemoteServerTxn : {
                SendRemoteServerTxn(ctx, epoch, to_whom, txn, txn_type);
                break;
            }
            case proto::TxnType::BackUpTxn : {
                MessageSendHandler::SendBackUpTxn(ctx, epoch, txn, txn_type);
                break;
            }
            case proto::TxnType::BackUpACK :
            case proto::TxnType::AbortSetACK :
            case proto::TxnType::InsertSetACK :
            case proto::TxnType::EpochShardingACK : {
                SendACK(ctx, epoch, to_whom, txn, txn_type);
                break;
            }
            case  proto::TxnType::EpochLogPushDownComplete : {
                SendMessageToAll(ctx, epoch, txn_type);
                break;
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
        return true;
    }

    bool MessageSendHandler::SendRemoteServerTxn(Context& ctx, uint64_t &epoch, uint64_t& to_whom, proto::Transaction& txn, proto::TxnType& txn_type) {
        if (to_whom == ctx.txn_node_ip_index) assert(false);
        auto msg = std::make_unique<proto::Message>();
        auto* txn_temp = msg->mutable_txn();
        *(txn_temp) = txn;
        txn_temp->set_txn_type(txn_type);
        auto serialized_txn_str_ptr = std::make_unique<std::string>();
        auto res = Gzip(msg.get(), serialized_txn_str_ptr.get());
        assert(res);
        assert(!serialized_txn_str_ptr->empty());
        send_to_server_queue->enqueue( std::make_unique<send_params>(to_whom, 0, "",epoch, txn_type,
                                              std::move(serialized_txn_str_ptr), nullptr));
        send_to_server_queue->enqueue( std::make_unique<send_params>(to_whom, 0, "",epoch, proto::TxnType::NullMark,
                                              nullptr, nullptr));

        return true;
    }

    bool MessageSendHandler::SendBackUpTxn(Context &ctx, uint64_t &epoch, proto::Transaction &txn,
                                           proto::TxnType &txn_type) {
        auto msg = std::make_unique<proto::Message>();
        auto* txn_temp = msg->mutable_txn();
        *(txn_temp) = txn;
        txn_temp->set_txn_type(txn_type);
        auto serialized_txn_str_ptr = std::make_unique<std::string>();
        auto res = Gzip(msg.get(), serialized_txn_str_ptr.get());
        assert(res);
        assert(!serialized_txn_str_ptr->empty());
        auto to_id = ctx.txn_node_ip_index + 1;
        for (uint64_t i = 0; i < ctx.kBackUpNum; i++) {
            to_id = (to_id + i) % ctx.kTxnNodeNum;
            if (to_id == ctx.txn_node_ip_index) continue;
            send_to_server_queue->enqueue(std::make_unique<send_params>(to_id, 0, "",epoch, txn_type,std::make_unique<std::string>(*serialized_txn_str_ptr), nullptr));
            send_to_server_queue->enqueue(std::make_unique<send_params>(to_id, 0, "",epoch, proto::TxnType::NullMark,nullptr, nullptr));
        }
        return true;
    }

    bool MessageSendHandler::SendACK(Context& ctx, uint64_t &epoch, uint64_t& to_whom, proto::Transaction& txn, proto::TxnType& txn_type) {
        if(to_whom == ctx.txn_node_ip_index) return true;
        auto msg = std::make_unique<proto::Message>();
        auto* txn_end = msg->mutable_txn();
        txn_end->set_server_id(ctx.txn_node_ip_index);
        txn_end->set_txn_type(txn_type);
        txn_end->set_commit_epoch(epoch);
        txn_end->set_sharding_id(0);
        std::vector<std::string> keys, values;
        auto serialized_txn_str_ptr = std::make_unique<std::string>();
        auto res = Gzip(msg.get(), serialized_txn_str_ptr.get());
        assert(res);
        send_to_server_queue->enqueue(std::make_unique<send_params>(to_whom, 0, "", epoch, txn_type, std::move(serialized_txn_str_ptr), nullptr));
        send_to_server_queue->enqueue(std::make_unique<send_params>(to_whom, 0, "", epoch, proto::TxnType::NullMark, nullptr, nullptr));
        return true;
    }

    bool
    MessageSendHandler::SendMessageToAll(Context &ctx, uint64_t &epoch, proto::TxnType &txn_type) {
        auto msg = std::make_unique<proto::Message>();
        auto* txn_end = msg->mutable_txn();
        txn_end->set_server_id(ctx.txn_node_ip_index);
        txn_end->set_txn_type(txn_type);
        txn_end->set_commit_epoch(epoch);
        txn_end->set_sharding_id(0);
        auto serialized_txn_str_ptr = std::make_unique<std::string>();
        auto res = Gzip(msg.get(), serialized_txn_str_ptr.get());
        assert(res);
        auto to_id = ctx.txn_node_ip_index + 1;
        for (uint64_t i = 0; i < ctx.kBackUpNum; i++) {
            to_id = (to_id + i) % ctx.kTxnNodeNum;
            if (to_id == ctx.txn_node_ip_index) continue;/// send to everyone
            send_to_server_queue->enqueue(std::make_unique<send_params>(to_id, 0, "", epoch, proto::TxnType::InsertSet, std::move(serialized_txn_str_ptr), nullptr));
        }
        send_to_server_queue->enqueue(std::make_unique<send_params>(to_id, 0, "", epoch, proto::TxnType::NullMark, nullptr, nullptr));
        return true;
    }



    ///一下函数都由0号线程执行
    bool MessageSendHandler::SendEpochEndMessage(Context &ctx) {
        for(uint64_t sharding_id = 0; sharding_id < ctx.kTxnNodeNum; sharding_id ++) { /// send to everyone  sharding_num == TxnNodeNum
        ///检查当前server(sharding_id)的第send_epoch的endFlag是否能够发送
            if(sharding_id == ctx.txn_node_ip_index) continue;
            while (MessageReceiveHandler::IsShardingSendFinish(sharding_send_epoch[sharding_id], sharding_id)) {
                auto msg = std::make_unique<proto::Message>();
                auto *txn_end = msg->mutable_txn();
                txn_end->set_server_id(ctx.txn_node_ip_index);
                txn_end->set_txn_type(proto::TxnType::EpochEndFlag);
                txn_end->set_commit_epoch(sharding_send_epoch[sharding_id]);
                txn_end->set_sharding_id(sharding_id);
                txn_end->set_csn(static_cast<uint64_t>(MessageReceiveHandler::sharding_should_send_txn_num.GetCount(
                        sharding_send_epoch[sharding_id]))); /// 不同server由不同的分片数量
                auto serialized_txn_str_ptr = std::make_unique<std::string>();
                auto res = Gzip(msg.get(), serialized_txn_str_ptr.get());
                assert(res);
//                printf("send epoch end flag server %lu epoch %lu\n",sharding_id, sharding_send_epoch[sharding_id]);
                send_to_server_queue->enqueue(std::make_unique<send_params>(sharding_id, 0, "", sharding_send_epoch[sharding_id], proto::TxnType::EpochEndFlag, std::move(serialized_txn_str_ptr), nullptr));
                send_to_server_queue->enqueue(std::make_unique<send_params>(sharding_id, 0, "", sharding_send_epoch[sharding_id], proto::TxnType::NullMark, nullptr, nullptr));
                sharding_send_epoch[sharding_id]++;
            }
        }
        return true;
    }

    bool MessageSendHandler::SendBackUpEpochEndMessage(Context &ctx) {
        while(MessageReceiveHandler::IsBackUpSendFinish(backup_send_epoch, ctx)) {
            auto msg = std::make_unique<proto::Message>();
            auto* txn_end = msg->mutable_txn();
            txn_end->set_server_id(ctx.txn_node_ip_index);
            txn_end->set_txn_type(proto::TxnType::BackUpEpochEndFlag);
            txn_end->set_commit_epoch(backup_send_epoch);
            txn_end->set_sharding_id(0);
            txn_end->set_csn(static_cast<uint64_t>(MessageReceiveHandler::backup_should_send_txn_num.GetCount(backup_send_epoch)));
            auto serialized_txn_str_ptr = std::make_unique<std::string>();
            auto res = Gzip(msg.get(), serialized_txn_str_ptr.get());
            assert(res);
            auto to_id = ctx.txn_node_ip_index + 1;
            for(uint64_t i = 0; i < ctx.kBackUpNum; i ++) { /// send to i+1, i+2...i+kBackNum-1
                to_id = (to_id + i) % ctx.kTxnNodeNum;
                if(to_id == ctx.txn_node_ip_index) continue;
                auto str_copy = std::make_unique<std::string>(*serialized_txn_str_ptr);
                send_to_server_queue->enqueue(std::make_unique<send_params>(to_id, 0, "", backup_send_epoch, proto::TxnType::BackUpEpochEndFlag, std::move(str_copy), nullptr));
            }
            printf("send epoch backup end flag epoch %lu\n",backup_send_epoch);
            send_to_server_queue->enqueue(std::make_unique<send_params>(to_id, 0, "", backup_send_epoch, proto::TxnType::NullMark, nullptr, nullptr));
            backup_send_epoch ++;
        }
        return true;
    }

    bool MessageSendHandler::SendAbortSet(Context &ctx) {
        for(abort_set_send_epoch = EpochManager::GetLogicalEpoch(); abort_set_send_epoch < EpochManager::GetPhysicalEpoch(); abort_set_send_epoch ++) {
            if(EpochManager::IsShardingMergeComplete(abort_set_send_epoch)) continue;
            if (MessageReceiveHandler::IsRemoteShardingPackReceiveComplete(abort_set_send_epoch, ctx) &&
                MessageReceiveHandler::IsRemoteShardingTxnReceiveComplete(abort_set_send_epoch, ctx) &&
                MessageReceiveHandler::IsEpochTxnEnqueued_MergeQueue(abort_set_send_epoch, ctx) &&
                Merger::IsEpochMergeComplete(abort_set_send_epoch, ctx) &&
                MessageReceiveHandler::IsBackUpSendFinish(abort_set_send_epoch, ctx) &&
                MessageReceiveHandler::IsBackUpACKReceiveComplete(abort_set_send_epoch, ctx)) {

                auto msg = std::make_unique<proto::Message>();
                auto *txn_end = msg->mutable_txn();
                txn_end->set_server_id(ctx.txn_node_ip_index);
                txn_end->set_txn_type(proto::TxnType::AbortSet);
                txn_end->set_commit_epoch(abort_set_send_epoch);
                txn_end->set_sharding_id(0);
                std::vector<std::string> keys, values;
                Merger::local_epoch_abort_txn_set[abort_set_send_epoch % ctx.kCacheMaxLength]->getValue(keys,
                                                                                                              values);
                for (uint64_t i = 0; i < keys.size(); i++) {
                    auto row = txn_end->add_row();
                    row->set_key(keys[i]);
                    row->set_data(values[i]);
                }
                auto serialized_txn_str_ptr = std::make_unique<std::string>();
                auto res = Gzip(msg.get(), serialized_txn_str_ptr.get());
                assert(res);
                for (uint64_t i = 0; i < ctx.kTxnNodeNum; i++) { /// send to everyone
                    if (i == ctx.txn_node_ip_index) continue;
                    auto str_copy = std::make_unique<std::string>(*serialized_txn_str_ptr);
                    send_to_server_queue->enqueue( std::make_unique<send_params>(i, 0, "", abort_set_send_epoch, proto::TxnType::AbortSet,std::move(str_copy), nullptr));
                }
                printf("send epoch abort set flag epoch %lu\n",abort_set_send_epoch);
                send_to_server_queue->enqueue( std::make_unique<send_params>(0, 0, "", abort_set_send_epoch, proto::TxnType::NullMark, nullptr, nullptr));
                EpochManager::SetShardingMergeComplete(abort_set_send_epoch, true);
            }
        }
        return true;
    }

    bool MessageSendHandler::SendInsertSet(Context &ctx) {
        auto msg = std::make_unique<proto::Message>();
        auto* txn_end = msg->mutable_txn();
        txn_end->set_server_id(ctx.txn_node_ip_index);
        txn_end->set_txn_type(proto::TxnType::InsertSet);
        txn_end->set_commit_epoch(insert_set_send_epoch);
        txn_end->set_sharding_id(0);
        std::vector<std::string> keys, values;
        Merger::epoch_insert_set[insert_set_send_epoch % ctx.kCacheMaxLength]->getValue(keys, values);
        for(uint64_t i = 0; i < keys.size(); i ++) {
            auto row = txn_end->add_row();
            row->set_key(keys[i]);
            row->set_data(values[i]);
        }
        auto serialized_txn_str_ptr = std::make_unique<std::string>();
        auto res = Gzip(msg.get(), serialized_txn_str_ptr.get());
        assert(res);
        for(uint64_t i = 0; i < ctx.kTxnNodeNum; i ++) { /// send to everyone
            if(i == ctx.txn_node_ip_index) continue;
            auto str_copy = std::make_unique<std::string>(*serialized_txn_str_ptr);
            send_to_server_queue->enqueue(std::make_unique<send_params>(i, 0, "", insert_set_send_epoch, proto::TxnType::InsertSet, std::move(str_copy), nullptr));
        }
        send_to_server_queue->enqueue(std::make_unique<send_params>(0, 0, "", insert_set_send_epoch, proto::TxnType::NullMark, nullptr, nullptr));
        insert_set_send_epoch ++;
        return true;
    }


    void MessageSendHandler::Init(Context &ctx) {
        backup_send_epoch = 1;
        abort_set_send_epoch = 1;
        insert_set_send_epoch = 1;
        for(uint64_t i = 0; i < ctx.kTxnNodeNum; i ++) {
            sharding_send_epoch.push_back(1);
        }
    }

}