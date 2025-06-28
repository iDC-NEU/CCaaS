//
// Created by 周慰星 on 11/9/22.
//

#ifndef TAAS_EPOCH_MESSAGE_SEND_HANDLER_H
#define TAAS_EPOCH_MESSAGE_SEND_HANDLER_H

#pragma once

#include "proto/message.pb.h"

#include "message.h"
#include "epoch/epoch_manager.h"

#include "epoch_message_receive_handler.h"
#include "tools/context.h"

namespace Taas {

    class EpochMessageSendHandler {
    public:
        static std::atomic<uint64_t> TotalLatency, TotalTxnNum, TotalSuccessTxnNUm, TotalSuccessLatency;
        static bool SendTxnCommitResultToClient(const std::shared_ptr<proto::Transaction>& txn_ptr, proto::TxnState txn_state);
        static bool SendTxnToServer(uint64_t& epoch, uint64_t& to_whom, const std::shared_ptr<proto::Transaction>& txn_ptr, proto::TxnType txn_type);
        static bool SendRemoteServerTxn(uint64_t& epoch, uint64_t& to_whom, const std::shared_ptr<proto::Transaction>& txn_ptr, proto::TxnType txn_type);
        static bool SendBackUpTxn(uint64_t& epoch, const std::shared_ptr<proto::Transaction>& txn_ptr, proto::TxnType txn_type);
        static bool SendACK(uint64_t &epoch, uint64_t &to_whom, proto::TxnType txn_type);
        static bool SendMessageToAll(uint64_t& epoch, proto::TxnType txn_type);

        ///一下函数都由single one线程执行
        static void StaticInit();
        static void StaticClear();
        static std::vector<std::unique_ptr<std::atomic<uint64_t>>> shard_send_epoch, backup_send_epoch, abort_set_send_epoch, insert_set_send_epoch;
//        static uint64_t shard_sent_epoch, backup_sent_epoch, abort_sent_epoch, insert_set_sent_epoch, abort_set_sent_epoch;

        static void CheckAndSendEpochMessage();
        static void CheckAndSendEpochShardEndMessage();
        static bool SendEpochShardEndMessage(const uint64_t &txn_node_ip_index, const uint64_t &epoch, const uint64_t &kTxnNodeNum);
        static void CheckAndSendEpochRemoteServerEndMessage();
        static bool SendEpochRemoteServerEndMessage(const uint64_t &txn_node_ip_index, const uint64_t &epoch, const uint64_t &kTxnNodeNum);
        static void CheckAndSendAbortSet();
        static bool SendAbortSet(const uint64_t &txn_node_ip_index, const uint64_t &epoch);

    private:
        bool sleep_flag = false;
        std::unique_ptr<pack_params> pack_param;
        bool SendMetaInfo(const uint64_t& txn_node_ip_index, const uint64_t& epoch);
    };
}
#endif //TAAS_EPOCH_MESSAGE_SEND_HANDLER_H
