//
// Created by 周慰星 on 11/9/22.
//
#include "message/transmit.h"
#include "utils/utilities.h"
namespace Taas {

/**
 * @brief 将txn设置事务状态，并通过protobuf将Reply序列化，将序列化的结果放到send_to_client_queue中，等待发送给客户端
 *
 * @param ctx XML文件的配置信息
 * @param txn 等待回复给client的事务
 * @param txn_state 告诉client此txn的状态(Success or Abort)
 */
    bool MessageTransmitter::ReplyTxnStateToClient(Context &ctx, proto::Transaction &txn, proto::TxnState txn_state) {
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

/**
 * @brief 将txn通过protobuf序列化，并将序列化后的结果放到pack_txn_queue中，等待发送给peer txn node.
 *
 * @param ctx XML中的配置相关信息
 * @param txn 待发送的事务(Transaction)
 * @return true
 * @return false
 */
    bool MessageTransmitter::SendTxnToPack(Context &ctx, proto::Transaction &txn) {
        UNUSED_VALUE(ctx);
        auto msg = std::make_unique<proto::Message>();
        auto* txn_temp = msg->mutable_txn();
        *(txn_temp) = txn;
        txn_temp->set_txn_type(proto::TxnType::RemoteServerTxn);
        auto serialized_txn_str_ptr = std::make_unique<std::string>();
        auto res = Gzip(msg.get(), serialized_txn_str_ptr.get());
        assert(res);
        assert(!serialized_txn_str_ptr->empty() );
        if(pack_txn_queue.enqueue(std::move(std::make_unique<pack_params>(std::move(serialized_txn_str_ptr), nullptr, txn.commit_epoch())))) {
            if(pack_txn_queue.enqueue(std::move(std::make_unique<pack_params>(nullptr, nullptr, 0)))) {
                return true;
            }
            else {
                return false;
            }
        }
        else {
            return false;
        }
    }

    bool MessageTransmitter::SendEpochEndMessage(uint64_t& id, Context& ctx, uint64_t& send_epoch) {
        while(EpochManager::IsCurrentEpochFinished(send_epoch)) {
            auto msg = std::make_unique<proto::Message>();
            auto* txn_end = msg->mutable_txn();
            txn_end->set_server_id(ctx.txn_node_ip_index);
            txn_end->set_txn_type(proto::TxnType::EpochEndFlag);
            txn_end->set_commit_epoch(send_epoch);
            txn_end->set_csn(static_cast<uint64_t>(EpochManager::local_packed_txn_num.GetCount(send_epoch)));
            auto serialized_txn_str_ptr = std::make_unique<std::string>();
            auto res = Gzip(msg.get(), serialized_txn_str_ptr.get());
            assert(res);
            if(!send_to_server_queue.enqueue(std::move(
                    std::make_unique<send_params>(0, 0, "", std::move(serialized_txn_str_ptr))))) assert(false);
            if(!send_to_server_queue.enqueue(std::move(
                    std::make_unique<send_params>(0, 0, "", nullptr)))) assert(false); //防止moodycamel取不出
//        printf("MergeRequest 到达发送时间，发送 第%llu个Pack线程, epoch:%llu 共%llu : %llu, %llu\n",
//               id, send_epoch, EpochManager::local_packed_txn_num.GetCount(send_epoch),
//               EpochManager::local_should_pack_txn_num.GetCount(send_epoch),
//               EpochManager::local_abort_before_pack_txn_num.GetCount(send_epoch));
            send_epoch ++;
        }
        return true;
    }

    bool MessageTransmitter::SendEpochSerializedTxn(uint64_t& id, Context& ctx, uint64_t& send_epoch, std::unique_ptr<pack_params>& pack_param) {
        if(ctx.kTxnNodeNum == 1) {
            return true;
        }

        bool sleep_flag = false;
        while(pack_txn_queue.try_dequeue(pack_param)) {
            if(pack_param == nullptr || pack_param->str == nullptr) continue;
            if((EpochManager::GetLogicalEpoch() % ctx.kCacheMaxLength) ==  ((EpochManager::GetPhysicalEpoch() + 2) % ctx.kCacheMaxLength) ) assert(false);
            if(!send_to_server_queue.enqueue(std::move(std::make_unique<send_params>(0, 0, "", std::move(pack_param->str)))) ) assert(false);
            if(!send_to_server_queue.enqueue(std::move(std::make_unique<send_params>(0, 0, "", nullptr))) )  assert(false); //防止moodycamel取不出
            EpochManager::local_packed_txn_num.IncCount(pack_param->epoch, id, 1);
        }
        if(id == 0) sleep_flag = SendEpochEndMessage(id, ctx, send_epoch);
        return sleep_flag;
    }
}