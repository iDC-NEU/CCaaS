//
// Created by 周慰星 on 11/9/22.
//
#include "message/handler_send.h"
#include "message/handler_receive.h"
#include "tools/utilities.h"
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
        send_to_client_queue.enqueue(std::move(std::make_unique<send_params>(
                txn.client_txn_id(), txn.csn(), txn.client_ip(), std::move(serialized_txn_str_ptr))));
        send_to_client_queue.enqueue(std::move(std::make_unique<send_params>(
                0, 0, "", nullptr)));
        return true;
    }

    bool MessageSendHandler::SendTaskToPackThread(Context& ctx, uint64_t &epoch, proto::TxnType txn_type) {
        pack_txn_queue.enqueue(std::move(std::make_unique<pack_params>(0, 0, "",epoch,txn_type, nullptr, nullptr)));
        pack_txn_queue.enqueue(std::move(std::make_unique<pack_params>(0, 0, "", 0, txn_type, nullptr, nullptr)));
        return true;
    }

    bool MessageSendHandler::SendTxnToPackThread(Context& ctx, proto::Transaction& txn, proto::TxnType txn_type) {
        UNUSED_VALUE(ctx);
        auto msg = std::make_unique<proto::Message>();
        auto* txn_temp = msg->mutable_txn();
        *(txn_temp) = txn;
        txn_temp->set_txn_type(txn_type);
        auto serialized_txn_str_ptr = std::make_unique<std::string>();
        auto res = Gzip(msg.get(), serialized_txn_str_ptr.get());
        assert(res);
        assert(!serialized_txn_str_ptr->empty());
        pack_txn_queue.enqueue(std::move(std::make_unique<pack_params>(txn.sharding_id(), 0, "", txn.commit_epoch(),txn_type, std::move(serialized_txn_str_ptr), nullptr)));
        pack_txn_queue.enqueue(std::move(std::make_unique<pack_params>(0, 0, "", 0, txn_type, nullptr, nullptr)));
        return true;
    }

    bool MessageSendHandler::SendTxnToSendThread(uint64_t& id, Context& ctx) {
        auto sleep_flag = false;
        uint64_t backup_send_epoch = 1, abort_set_send_epoch = 1, insert_set_send_epoch = 1;
        std::vector<uint64_t> sharding_send_epoch;
        for(int i = 0; i < ctx.kTxnNodeNum; i ++) {
            sharding_send_epoch.push_back(1);
        }
        std::unique_ptr<pack_params> pack_param;
        if (ctx.kTxnNodeNum == 1) {
            while (!EpochManager::IsTimerStop()) {
                pack_txn_queue.wait_dequeue(pack_param);
            }
        }
        while (!EpochManager::IsTimerStop()) {
            sleep_flag = false;
            while(pack_txn_queue.try_dequeue(pack_param)) {
                if(pack_param == nullptr || pack_param->str == nullptr) continue;
                if((EpochManager::GetLogicalEpoch() % ctx.kCacheMaxLength) ==  ((EpochManager::GetPhysicalEpoch() + 55) % ctx.kCacheMaxLength) ) {
                    printf("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
                    printf("+++++++++++++++Fata : Cache Size exceeded!!! +++++++++++++++++++++\n");
                    printf("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
                    assert(false);
                }

                if(pack_param->type == proto::TxnType::RemoteServerTxn) {
                    send_to_server_queue.enqueue(std::move(std::make_unique<send_params>(0, 0, "", std::move(pack_param->str))));
                    MessageReceiveHandler::sharding_send_txn_num.IncCount(pack_param->epoch, pack_param->id, 1);
                }
                else if(pack_param->type == proto::TxnType::BackUpTxn) {
                    auto to_id = ctx.txn_node_ip_index + 1;
                    for(int i = 0; i < ctx.kBackUpNum; i ++) {
                        to_id = (to_id + i) % ctx.kTxnNodeNum;
                        send_to_server_queue.enqueue(std::move(
                                std::make_unique<send_params>(to_id, 0, "", std::make_unique<std::string>(*pack_param->str))));
                    }
                    MessageReceiveHandler::backup_send_txn_num.IncCount(pack_param->epoch, pack_param->id, 1);
                }
                else if(pack_param->type == proto::TxnType::AbortSet) {
                    SendAbortSet(id, ctx, pack_param->epoch);
                }
                else if(pack_param->type == proto::TxnType::InsertSet) {
                    SendInsertSet(id, ctx, pack_param->epoch);
                }
                sleep_flag = true;
            }
            if(id == 0) {
                sleep_flag = sleep_flag | MessageSendHandler::SendEpochEndMessage(id, ctx, sharding_send_epoch);
                sleep_flag = sleep_flag | MessageSendHandler::SendBackUpEpochEndMessage(id, ctx, backup_send_epoch);
            }
            if (!sleep_flag)
                usleep(200);
        }
    }

    bool MessageSendHandler::SendEpochEndMessage(uint64_t& id, Context& ctx, std::vector<uint64_t>& send_epoch) {
        for(int sharding_id = 0; sharding_id < ctx.kTxnNodeNum; sharding_id ++) { /// send to everyone  sharding_num == TxnNodeNum
            while (MessageReceiveHandler::IsShardingSendFinish(send_epoch[sharding_id], sharding_id)) {
                auto msg = std::make_unique<proto::Message>();
                auto *txn_end = msg->mutable_txn();
                txn_end->set_server_id(ctx.txn_node_ip_index);
                txn_end->set_txn_type(proto::TxnType::EpochEndFlag);
                txn_end->set_commit_epoch(send_epoch[sharding_id]);
                txn_end->set_csn(static_cast<uint64_t>(MessageReceiveHandler::sharding_should_send_txn_num.GetCount(
                        send_epoch[sharding_id]))); /// bu tong server you bu tong de txn shu liang
                auto serialized_txn_str_ptr = std::make_unique<std::string>();
                auto res = Gzip(msg.get(), serialized_txn_str_ptr.get());
                assert(res);
                !send_to_server_queue.enqueue(std::move(
                        std::make_unique<send_params>(sharding_id, 0, "", std::move(serialized_txn_str_ptr))));
                !send_to_server_queue.enqueue(std::move(
                        std::make_unique<send_params>(sharding_id, 0, "", nullptr)));
                //        printf("MergeRequest 到达发送时间，发送 第%llu个Pack线程, epoch:%llu 共%llu : %llu, %llu\n",
                //               id, send_epoch, EpochManager::local_packed_txn_num.GetCount(send_epoch),
                //               EpochManager::local_should_pack_txn_num.GetCount(send_epoch),
                //               EpochManager::local_abort_before_pack_txn_num.GetCount(send_epoch));
                send_epoch[sharding_id]++;
            }
        }
        return true;
    }

    bool MessageSendHandler::SendBackUpEpochEndMessage(uint64_t& id, Context& ctx, uint64_t& send_epoch) {
        while(MessageReceiveHandler::IsBackUpSendFinish(send_epoch)) {
            auto msg = std::make_unique<proto::Message>();
            auto* txn_end = msg->mutable_txn();
            txn_end->set_server_id(ctx.txn_node_ip_index);
            txn_end->set_txn_type(proto::TxnType::BackUpEpochEndFlag);
            txn_end->set_commit_epoch(send_epoch);
            txn_end->set_csn(static_cast<uint64_t>(MessageReceiveHandler::backup_should_send_txn_num.GetCount(send_epoch)));
            auto serialized_txn_str_ptr = std::make_unique<std::string>();
            auto res = Gzip(msg.get(), serialized_txn_str_ptr.get());
            assert(res);
            auto to_id = ctx.txn_node_ip_index + 1;
            for(int i = 0; i < ctx.kBackUpNum; i ++) { /// send to i+1, i+2...i+kBackNum-1
                to_id = (to_id + i) % ctx.kTxnNodeNum;
                send_to_server_queue.enqueue(std::move(
                        std::make_unique<send_params>(to_id, 0, "", std::make_unique<std::string>(*serialized_txn_str_ptr))));
            }
            send_to_server_queue.enqueue(std::move(
                    std::make_unique<send_params>(to_id, 0, "", nullptr)));
//        printf("MergeRequest 到达发送时间，发送 第%llu个Pack线程, epoch:%llu 共%llu : %llu, %llu\n",
//               id, send_epoch, EpochManager::local_packed_txn_num.GetCount(send_epoch),
//               EpochManager::local_should_pack_txn_num.GetCount(send_epoch),
//               EpochManager::local_abort_before_pack_txn_num.GetCount(send_epoch));
            send_epoch ++;
        }
        return true;
    }

    bool MessageSendHandler::SendAbortSet(uint64_t &id, Context &ctx, uint64_t &send_epoch) {
        while(MessageReceiveHandler::IsBackUpSendFinish(send_epoch)) {
            auto msg = std::make_unique<proto::Message>();
            auto* txn_end = msg->mutable_txn();
            txn_end->set_server_id(ctx.txn_node_ip_index);
            txn_end->set_txn_type(proto::TxnType::AbortSet);
            txn_end->set_commit_epoch(send_epoch);
            std::vector<std::string> keys, values;
            EpochManager::local_epoch_abort_txn_set[send_epoch % ctx.kCacheMaxLength]->getValue(keys, values);
            for(int i = 0; i < keys.size(); i ++) {
                auto row = txn_end->add_row();
                row->set_key(keys[i]);
                row->set_data(values[i]);
            }
            auto serialized_txn_str_ptr = std::make_unique<std::string>();
            auto res = Gzip(msg.get(), serialized_txn_str_ptr.get());
            assert(res);
            for(int i = 0; i < ctx.kTxnNodeNum; i ++) { /// send to everyone
                if(i == ctx.txn_node_ip_index) continue;
                send_to_server_queue.enqueue(std::move(
                        std::make_unique<send_params>(i, 0, "", std::make_unique<std::string>(*serialized_txn_str_ptr))));
            }
            send_to_server_queue.enqueue(std::move(
                    std::make_unique<send_params>(0, 0, "", nullptr)));
//        printf("MergeRequest 到达发送时间，发送 第%llu个Pack线程, epoch:%llu 共%llu : %llu, %llu\n",
//               id, send_epoch, EpochManager::local_packed_txn_num.GetCount(send_epoch),
//               EpochManager::local_should_pack_txn_num.GetCount(send_epoch),
//               EpochManager::local_abort_before_pack_txn_num.GetCount(send_epoch));
            send_epoch ++;
        }
        return true;
    }

    bool MessageSendHandler::SendInsertSet(uint64_t &id, Context &ctx, uint64_t &send_epoch) {
        while(MessageReceiveHandler::IsBackUpSendFinish(send_epoch)) {
            auto msg = std::make_unique<proto::Message>();
            auto* txn_end = msg->mutable_txn();
            txn_end->set_server_id(ctx.txn_node_ip_index);
            txn_end->set_txn_type(proto::TxnType::InsertSet);
            txn_end->set_commit_epoch(send_epoch);
            std::vector<std::string> keys, values;
            EpochManager::epoch_insert_set[send_epoch % ctx.kCacheMaxLength]->getValue(keys, values);
            for(int i = 0; i < keys.size(); i ++) {
                auto row = txn_end->add_row();
                row->set_key(keys[i]);
                row->set_data(values[i]);
            }
            auto serialized_txn_str_ptr = std::make_unique<std::string>();
            auto res = Gzip(msg.get(), serialized_txn_str_ptr.get());
            assert(res);
            for(int i = 0; i < ctx.kTxnNodeNum; i ++) { /// send to everyone
                if(i == ctx.txn_node_ip_index) continue;
                send_to_server_queue.enqueue(std::move(
                        std::make_unique<send_params>(i, 0, "", std::make_unique<std::string>(*serialized_txn_str_ptr))));
            }
            send_to_server_queue.enqueue(std::move(
                    std::make_unique<send_params>(0, 0, "", nullptr)));
//        printf("MergeRequest 到达发送时间，发送 第%llu个Pack线程, epoch:%llu 共%llu : %llu, %llu\n",
//               id, send_epoch, EpochManager::local_packed_txn_num.GetCount(send_epoch),
//               EpochManager::local_should_pack_txn_num.GetCount(send_epoch),
//               EpochManager::local_abort_before_pack_txn_num.GetCount(send_epoch));
            send_epoch ++;
        }
        return true;
    }

}